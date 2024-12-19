from enum import Enum
import signal
import platform
import logging
import signal
from types import FrameType
from typing import Generator, List, Optional, Tuple, Type, Callable, cast
import tornado.web
from x2.c3.hwm import random_port
from x2.c3.periodic import PeriodicTask, run_all
import asyncio
import json
from tornado.httpclient import AsyncHTTPClient


log = logging.getLogger(__name__)

async def get_json(url):
    response = await AsyncHTTPClient().fetch(url)
    return json.loads(response.body)


class AppService:
    """Service that manages routes and periodic tasks for an app"""
    
    def __init__(self):
        self._routes: List[Tuple[str, Type[tornado.web.RequestHandler]]] = []
        self._periodic_tasks: List[PeriodicTask] = []
        self.app_state:AppState = None
        self.app:App = None

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

    def on_start(self) -> None:
        """Start the service"""
        pass

    def on_stop(self) -> None:
        """Stop the service"""
        pass
    
    def __repr__(self):
        return f"AppService(routes={self._routes}, periodic_tasks={self._periodic_tasks})"

class PortSeekStrategy(Enum):
    """
    Strategy for finding an available port

    >>> PortSeekStrategy.SEQUENTIAL.next_port(8080)
    8081
    >>> PortSeekStrategy.BAILOUT.next_port(8080)
    >>>
    
    """
    SEQUENTIAL = 1
    RANDOM = 2
    BAILOUT = 3

    def next_port(self, port:Optional[int]) -> Optional[int]:
        if self == PortSeekStrategy.RANDOM:
            return random_port()
        elif self == PortSeekStrategy.SEQUENTIAL:
            return port + 1
        return None


def _get_bind_errno() -> int:
    """
    Returns the platform-specific errno for 'Address already in use' error

    MacOS: OSError: [Errno 48] Address already in use
    Linux: OSError: [Errno 98] Address already in use
    Windows: OSError: [WinError 10048] Only one usage of each socket address 
                      (protocol/network address/port) is normally permitted
    
    Returns:
        int: errno value for the current platform
            - 48 on MacOS
            - 98 on Linux  
            - 10048 on Windows
        Use linux errno as fallback
    """
    errnos = {
        'darwin': 48,
        'linux': 98,
        'windows': 10048
    }
    system = platform.system().lower()
    return errnos.get(system, errnos['linux'])
 
BIND_ERRNO = _get_bind_errno()

class AppState:
    app_services:List[AppService]
    app:"App"

    def __init__(self, *app_services:AppService, port:Optional[int] = None, port_seek:Optional[PortSeekStrategy] = None):
        self.app_services = list(app_services)
        self.port = port
        self.port_seek = port_seek if port_seek is not None else ( PortSeekStrategy.RANDOM if port is None else PortSeekStrategy.SEQUENTIAL)
        self.app = None


    def tornado_app(self) -> tornado.web.Application:
        routes = []
        for service in self.app_services:
            routes.extend(service.get_routes())
        return tornado.web.Application(cast(tornado.routing._RuleList,routes))
    
    def periodic_tasks(self)->Generator[PeriodicTask, None, None]:
        for service in self.app_services:
            for t in service.get_periodic_tasks():
                yield t
    
    def listen(self, max_attempts:int = 10):
        app = self.tornado_app()
        reset_port = self.port is None
        for _ in range(max_attempts):
            if reset_port:
                self.port = self.port_seek.next_port(self.port)
            try:
                log.debug(f"Trying to listen on port {self.port}")
                app.listen(self.port)
                return
            except OSError as e:

                if self.port_seek == PortSeekStrategy.BAILOUT or e.errno != BIND_ERRNO:
                    log.error(f"Failed to listen on port {self.port}: {e}")
                    raise e
                else:
                    reset_port = True
        raise ValueError("Failed to find an available port after max_attempts", max_attempts) 
    
    def on_start(self):
        for service in self.app_services:
            service.app_state = self
            service.app = self.app
            service.on_start()

    def on_stop(self):
        for service in self.app_services:
            service.on_stop()

    def __repr__(self):
        return f"AppState(port={self.port}, port_seek={self.port_seek}, app_services={self.app_services})"


class App:
    def __init__(self, name:str, *app_states:AppState, shutdown_event:Optional[asyncio.Event] = None):
        self.name = name
        self.app_states = list(app_states)
        self.shutdown_event = shutdown_event
        self._started = False
        self._stopping: Optional[bool] = None

    def periodic_tasks(self)->List[PeriodicTask]:
        """ Return a list of tuples where the first element is the frequency in seconds 
        and the second element is the function to call.
        """
        tasks:List[PeriodicTask] = []
        for app_state in self.app_states:   
            tasks.extend(app_state.periodic_tasks())
        return tasks
    
    def on_start(self):
        if self._started:
            return
        self._started = True
        for app_state in self.app_states:
            app_state.app = self
            app_state.on_start()

    def on_stop(self):
        if not self._started:
            return
        self._stopping = True
        for app_state in self.app_states:
            app_state.on_stop()
        self._stopping = False
        self._started = False

    @property 
    def is_running(self) -> bool:
        """Check if app is currently running"""
        return self._started and not self._stopping

    def shutdown(self, *args, **kwargs):
        log.info(f"Stopping {self.name}!")
        self.shutdown_event.set()



    
    async def run(self, max_attempts_to_listen=10):
        assert not self._started
        assert self._stopping is None, "App was already running"
        if self.shutdown_event is None:
            self.shutdown_event = asyncio.Event()

        for app_state in self.app_states:
            app_state.listen(max_attempts=max_attempts_to_listen)

        self.on_start()
        try:
            loop = asyncio.get_event_loop()
            signal.signal(signal.SIGINT, self.shutdown)
            signal.signal(signal.SIGTERM, self.shutdown)
            tasks = self.periodic_tasks()
            if len(tasks):
                asyncio.create_task(run_all(*tasks, shutdown_event=self.shutdown_event))
            await self.shutdown_event.wait()
        finally:
            self.on_stop()

