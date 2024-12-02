#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from enum import Enum
import inspect
import random
import time
import json
from typing import Any, Callable, Dict, List, Optional, Tuple, cast, Type
import tornado
from tornado.httpclient import AsyncHTTPClient
import signal
import multiprocessing as mp
import time
import logging

from x2.c3.hwm import random_port
from x2.c3.hwm.wep import WorkerInitiationRequest
from x2.c3.periodic import PeriodicTask, run_all

log = logging.getLogger(__name__)



async def get_status(port, host="localhost"):
    """Fetch status json from worker."""
    url = f"http://{host}:{port}/status"
    response = await AsyncHTTPClient().fetch(url)
    return json.loads(response.body)


class AppService:
    """Service that manages routes and periodic tasks for an app"""
    
    def __init__(self):
        self._routes: List[Tuple[str, Type[tornado.web.RequestHandler]]] = []
        self._periodic_tasks: List[PeriodicTask] = []
        self._shutdown_handlers: List[Callable] = []
        self._started = False
        self._stopping = False

    def add_route(self, pattern: str, handler:Type[tornado.web.RequestHandler]) -> None:
        """Add a route pattern and handler"""
        self._routes.append((pattern, handler))

    def add_periodic(self, interval: int, fn: Callable) -> None:
        """Add a periodic task that runs at the specified interval"""
        task = PeriodicTask(interval, fn)
        self._periodic_tasks.append(task)

    def get_routes(self) -> List[Tuple[str, Type[tornado.web.RequestHandler]]]:
        """Get all registered routes"""
        return self._routes

    def get_periodic_tasks(self) -> List[PeriodicTask]:
        """Get all registered periodic tasks"""
        return self._periodic_tasks

    def add_shutdown_handler(self, handler: Callable) -> None:
        """Add a handler to be called during shutdown"""
        self._shutdown_handlers.append(handler)

    async def on_start(self) -> None:
        """Start the service"""
        if self._started:
            return
        self._started = True
        self._stopping = False

    async def on_stop(self) -> None:
        """Stop the service and run shutdown handlers"""
        if self._stopping or not self._started:
            return
        
        self._stopping = True
        
        # Run shutdown handlers in reverse order
        for handler in reversed(self._shutdown_handlers):
            try:
                if inspect.iscoroutinefunction(handler):
                    await handler()
                else:
                    handler()
            except Exception as e:
                log.error(f"Error running shutdown handler: {e}")
        
        self._started = False
        self._stopping = False

    @property 
    def is_running(self) -> bool:
        """Check if service is currently running"""
        return self._started and not self._stopping


class EndpointService(AppService):
    def __init__(self, init_req:WorkerInitiationRequest):
        super().__init__()

        class WorkerStatusHandler(tornado.web.RequestHandler):
            def get(self):
                self.write(json.dumps({"ok":True}))

        self.add_route(r"/status", WorkerStatusHandler)


class AppState:
    name:str 

    def __init__(self, name, *app_services:AppService):
        self.name = name
        self.app_services = app_services

    def tornado_app(self) -> tornado.web.Application:
        routes = []
        for service in self.app_services:
            routes.extend(service.get_routes())
        return tornado.web.Application(cast(tornado.routing._RuleList,routes))

    def periodic_tasks(self)->List[PeriodicTask]:
        """ Return a list of tuples where the first element is the frequency in seconds 
        and the second element is the function to call.
        """
        tasks = []
        for service in self.app_services:
            tasks.extend(service.get_periodic_tasks())
        return tasks


    async def start_app(self, port:int, shutdown_event:Optional[asyncio.Event] = None):
        app = self.tornado_app()
        app.listen(port)
        if shutdown_event is None:
            shutdown_event = asyncio.Event()

        def shutdown():
            log.info(f"Stopping {self.name}!")
            shutdown_event.set()

        loop = asyncio.get_event_loop()
        loop.add_signal_handler(signal.SIGINT, shutdown)
        loop.add_signal_handler(signal.SIGTERM, shutdown)
        asyncio.create_task(run_all(*self.periodic_tasks()))
        await shutdown_event.wait()


class _C3Mount(tornado.web.RequestHandler):
    # SUPPORTED_METHODS = ("GET",)

    def get(self, path):
        print(path)
        self.finish(str(path))


class _ContentHandler(tornado.web.RequestHandler):
    # SUPPORTED_METHODS = ("GET", "POST", "PUT")

    def put(self):
        print("PUT", self.request.body)
        self.finish(self.request.body)

    def post(self):
        print("POST", self.request.body)
        self.finish(self.request.body)

    def get(self, path):
        print("GET", self.request.body)
        self.finish(str(path))


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Hello, world")

class OrchestratorApp(AppState):
    def tornado_app(self):
        return tornado.web.Application([
            (r"/c3/mount/(.*)", _C3Mount),
            (r"/content/(.*)", _ContentHandler),
            (r"/", MainHandler),
        ])


class WorkerApp(AppState):
    def tornado_app(self):
        return tornado.web.Application()

APPS = {
    "orchestrator": OrchestratorApp,
    "worker": WorkerApp,
}

class WorkerStatus(Enum):
    STARTED = 1
    READY = 2
    SHUTDOWN = 3
    STOPPED = 4


class Worker:
    def __init__(self):
        self.port = random_port()
        self.process = mp.Process(target=run_app, args=("worker", self.port))
        self.process.start()
        print(f"Worker started on port {self.port}")
        self.state = WorkerStatus.STARTED
        self.remote_status = None

    async def get_remote_status(self)->Dict[str,Any]:
        self.remote_status = await get_status(self.port)
        self.remote_status['updated_at'] = time.time()
        return self.remote_status

    def elapsed_time_since_last_heartbeat(self):
        return time.time() - self.remote_status['updated_at'] if self.remote_status else int(1e6)
    
    def is_hearbeat_time(self, interval=30):
        return self.elapsed_time_since_last_heartbeat() > interval

    def is_active(self):
        return (
            self.process.is_alive() and 
            self.remote_status is not None and 
            self.remote_status["ok"])


class WorkerPool:
    def __init__(self, min_workers=1, max_workers=4):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.workers:List[Worker] = []

    # def ensure_workers(self):
    #     for _ in range(self.min_workers):
    #         await self.run_worker()

    async def run_worker(self):
        worker = Worker()
        self.workers.append(worker)
        await asyncio.sleep(1)
        missing_hearbeats = 0
        while missing_hearbeats > 3 and worker.is_active():
            if worker.is_hearbeat_time():
                try:
                    await worker.get_remote_status()
                    if worker.is_active():
                        missing_hearbeats = 0
                except:
                    missing_hearbeats += 1
            await asyncio.sleep(10)
        # worker.terminate()


def run_app(app_name, port):
    log.info(f"Starting {app_name}")
    app = APPS[app_name](app_name)
    asyncio.run(app.start_app(port))
    log.info(f"Finished {app_name}")

if __name__ == "__main__":
    run_app("orchestrator", 7532)
