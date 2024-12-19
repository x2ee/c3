import asyncio
import json
import logging
from typing import Callable
import pytest
import tornado
from x2.c3.hwm.service import App, AppService, AppState, PortSeekStrategy, get_json, BIND_ERRNO


class OkService(AppService):
    """A service that returns ok"""

    def __init__(self):
        super().__init__()

        class WorkerStatusHandler(tornado.web.RequestHandler):
            def get(self):
                self.write(json.dumps({"ok":True}))

        self.add_route(r"/status", WorkerStatusHandler)

class CheckOkService(AppService):
    """A service that checks if the ok service is ok"""

    def __init__(self, *tasks:Callable):
        super().__init__()
        self_service = self
        for i, task in enumerate(tasks):
            self.add_periodic(i+1, task)

        class CheckOkHandler(tornado.web.RequestHandler):
            async def get(self):
                u= "http://localhost:8000/status"
                status = await get_json(u)
                self.write(json.dumps({
                    "my_port": self_service.app.app_states[1].port,
                    "status":{"url": u, "ok":status["ok"]},
                    "app_running":self_service.app.is_running
                }))

        self.add_route(r"/check_ok", CheckOkHandler)



class TerminateAppTask:
    def __init__(self, app:App, timeout:int):
        self.app = app
        self.timeout = timeout

    async def terminate_app(self):
        try:
            await asyncio.sleep(self.timeout)
            self.app.shutdown()
        except asyncio.CancelledError:
            pass

    def __enter__(self):
        self.task = asyncio.create_task(self.terminate_app())
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        if self.task and not self.task.done():
            self.task.cancel()


@pytest.mark.asyncio
async def test_ok_service():
    app = App("test", AppState(OkService(), port=8000))
    
    with TerminateAppTask(app, 5):
        
        class TestItTask:
            def __init__(self):
                self.ok = None
            
            async def test_it(self):
                self.status = await get_json("http://localhost:8000/status")
                self.ports = [ st.port for st in app.app_states ]
                self.check_ok = await get_json(f"http://localhost:{self.ports[1]}/check_ok")
            
            def test_ok(self):
                if hasattr(self, "check_ok"):
                    assert self.status["ok"] == True
                    assert self.check_ok["my_port"] == self.ports[1]
                    self.ok = True
                    app.shutdown()

        t = TestItTask()
        
        app.app_states.append(AppState(CheckOkService(t.test_it, t.test_ok)))
        await app.run()
        print(t.status, t.ports, t.check_ok)
        assert t.ok == True

@pytest.mark.asyncio
async def test_conflict(caplog):
    try:    
        app = App("test", AppState(OkService(), port=8075))
        app.app_states.append(AppState(CheckOkService(), port=8075, port_seek=PortSeekStrategy.BAILOUT))
        await app.run()
        assert False
    except OSError as e:
        assert e.errno == BIND_ERRNO

@pytest.mark.asyncio
async def test_sequential(caplog):
    caplog.set_level(logging.DEBUG, logger='x2.c3.hwm.service')
    app = App("test", AppState(OkService(), port=8090),AppState(CheckOkService(), port=8090))
    with TerminateAppTask(app, 1):
        await app.run()
        d1 = app.app_states[0].port - 8090
        d2 = app.app_states[1].port - app.app_states[0].port
        assert d1 >= 0
        assert 0 < d2 < 10
    ports_tried = [int(x[1]) for x in (r.message.split('Trying to listen on port ') for r in caplog.records) if len(x)==2 and x[0]=='']
    assert len(ports_tried) > 2 , f'Too few {ports_tried=}'   

@pytest.mark.asyncio
async def test_value_ex():
    try:
        app = App("test", AppState(OkService(), port=8090))
        with TerminateAppTask(app, 1):
            app.app_states.append(AppState(CheckOkService(), port=8090))
            await app.run(max_attempts_to_listen=1)
        assert False
    except ValueError as e:
        assert e.args == ('Failed to find an available port after max_attempts', 1)

