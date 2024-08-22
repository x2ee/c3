#!/usr/bin/env python
# -*- coding: utf-8 -*-
import asyncio
from enum import Enum
import json
from typing import Any, Callable, Dict, List, Tuple
import tornado
from tornado.httpclient import AsyncHTTPClient
import signal
import multiprocessing as mp
import random
import time
import logging

from x2.c3.periodic import PeriodicTask, run_all
log = logging.getLogger(__name__)


random.seed(time.time())

def random_port():
    """Return a random port number between 1024 and 65535 but give 
    higher probability port numbers above 49152 to avoid reserved ports.
    """
    if random.randint(1,3) == 3:
        return random.randint(1024, 65535)
    return random.randint(49152, 65535)


async def get_status(port, host="localhost"):
    """Fetch status json from worker. """
    url = f"http://{host}:{port}/status"
    response = await AsyncHTTPClient().fetch(url)
    return json.loads(response.body)


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


class WorkerStatusHandler(tornado.web.RequestHandler):
    def get(self):
        self.write(json.dumps({"ok":True}))


class AppState:
    name:str 

    def __init__(self, name):
        self.name = name
        self._init_some_more()

    def _init_some_more(self):
        pass

    def tornado_app(self) -> tornado.web.Application:
        raise NotImplementedError

    def periodic_tasks(self)->List[Tuple[int, Callable[[], Any]]]:
        """ Return a list of tuples where the first element is the frequency in seconds 
        and the second element is the function to call.
        """
        return []


    async def start_app(self, port, shutdown_event = None):
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
        asyncio.create_task(run_all(*[PeriodicTask(*v) for v in self.periodic_tasks()]))
        await shutdown_event.wait()


class OrchestratorApp(AppState):
    def tornado_app(self):
        return tornado.web.Application([
            (r"/c3/mount/(.*)", _C3Mount),
            (r"/content/(.*)", _ContentHandler),
            (r"/", MainHandler),
        ])


class WorkerApp(AppState):
    def tornado_app(self):
        return tornado.web.Application(
            [
                (r"/status", WorkerStatusHandler),
            ]
        )

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
