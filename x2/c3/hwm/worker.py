#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import annotations

import asyncio
import json
import logging
import multiprocessing as mp
import time
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Tuple, Type, cast

import tornado
import tornado.web
from tornado.httpclient import AsyncHTTPClient

from x2.c3.hwm import random_port
from x2.c3.hwm.service import AppService, AppState, get_json
from x2.c3.hwm.wep import WorkerInitiationRequest



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
    return await get_json(url)



class EndpointService(AppService):
    def __init__(self, init_req:WorkerInitiationRequest):
        super().__init__()

        class WorkerStatusHandler(tornado.web.RequestHandler):
            def get(self):
                self.write(json.dumps({"ok":True}))

        self.add_route(r"/status", WorkerStatusHandler)




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


class WorkerStatus(Enum):
    STARTED = 1
    READY = 2
    SHUTDOWN = 3
    STOPPED = 4


class Worker:
    def __init__(self):
        self.port = random_port()
        self.process = mp.Process(args=("worker", self.port))
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


