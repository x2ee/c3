import asyncio
import inspect
import sys
import time as tt
from typing import Any, Callable, Optional

import logging
log = logging.getLogger(__name__)

from datetime import datetime, timedelta, date, timezone
from enum import Enum
import re
from pathlib import Path
from typing import Union


YEAR_IN_DAYS = 365.256

def stamp_time() -> datetime:
    """return the current time in UTC
    >>> stamp_time().tzinfo
    datetime.timezone.utc
    """
    return datetime.now(timezone.utc)


class SimulatedTime:
    """
    >>> st = SimulatedTime()
    >>> st.get_datetime().tzinfo
    datetime.timezone.utc
    >>> cmp = lambda ss: abs((st.get_datetime()-stamp_time()).total_seconds()-ss) < 1e-3
    >>> cmp(0)
    True
    >>> st.set_offset(timedelta(days=1))
    >>> cmp(86400)
    True
    >>> st.set_offset(timedelta(days=1).total_seconds())
    >>> cmp(86400)
    True
    >>> st.set_now(stamp_time() + timedelta(days=1))
    >>> cmp(86400)
    True
    >>> st.set_now( (stamp_time() - timedelta(days=1)).timestamp() )
    >>> cmp(-86400)
    True
    >>> st.is_real_time()
    False
    >>> st.reset()
    >>> st.is_real_time()
    True
    """
    def __init__(self, offset: float=0.) -> None:
        self.offset = offset

    def time(self):
        return tt.time() + self.offset

    def set_offset(self, offset: Union[timedelta,float]):
        if isinstance(offset, timedelta):
            self.offset = offset.total_seconds()
        else:
            self.offset = offset

    def set_now(self, dt: Union[datetime,float]):
        if isinstance(dt, datetime):
            epoch = dt.timestamp()
        else:
            epoch = dt
        self.offset = epoch - tt.time()

    def reset(self):
        self.offset = 0.

    def is_real_time(self):
        return self.offset == 0.

    def get_datetime(self)->datetime:
        return datetime.fromtimestamp(self.time(), tz=timezone.utc)


stime: SimulatedTime = SimulatedTime()


class IntervalUnit(Enum):
    """
    >>> IntervalUnit.D
    IntervalUnit.D
    """
    D = 1
    W = 7
    M = YEAR_IN_DAYS / 12
    Q = YEAR_IN_DAYS / 4
    Y = YEAR_IN_DAYS

    @classmethod
    def from_string(cls, n: str) -> "IntervalUnit":
        return cls[n.upper()]

    def timedelta(self) -> timedelta:
        return timedelta(days=self.value)

    def __str__(self) -> str:
        return self.name

    def __repr__(self) -> str:
        return f"IntervalUnit.{str(self)}"


class Interval:
    _P = "".join(p.name for p in IntervalUnit)
    FREQ_RE = re.compile(r"(\d+)([" + _P + _P.lower() + "])")

    def __init__(self, multiplier: int, period: IntervalUnit) -> None:
        self.multiplier = multiplier
        self.period = period


    @classmethod
    def from_string_safe(cls, s: Union["Interval",str,None]) -> Optional["Interval"]:
        if s is None:
            return None
        if isinstance(s, Interval):
            return s
        return cls.from_string(s)

    @classmethod
    def from_string(cls, s: str) -> "Interval":
        m = cls.matcher(s)
        if m:
            n, p = m.groups()
            return cls(int(n), IntervalUnit.from_string(p))
        else:
            raise ValueError("Invalid frequency string", s)

    @classmethod
    def matcher(cls, s):
        return re.match(cls.FREQ_RE, s)

    def timedelta(self) -> timedelta:
        return self.multiplier * self.period.timedelta()

    def match(self, d: date, as_of: date) -> bool:
        return d <= as_of and d + self.timedelta() > as_of

    def find_file(
        self,
        path: Path,
        as_of: Union[date, datetime],
        suffix: str = ".csv",
    ) -> Path:
        ff = list(
            reversed(
                sorted(
                    (date_from_name(f.name), f)
                    for f in path.glob(f"*{suffix}")
                    if re.match(r"^\d{8}", f.name[:8])
                )
            )
        )
        for d, f in ff:
            if d <= as_of:
                if self.match(d, as_of):
                    return f
                else:
                    break
        return None
    
    def __str__(self) -> str:
        return f"{self.multiplier}{self.period}"
    
    def __repr__(self) -> str:
        return f'Interval({self.multiplier}, {self.period!r})'


def date_from_name(s):
    return date(int(s[:4]), int(s[4:6]), int(s[6:8]))


class Moment:
    """
    >>> m = Moment.start()
    >>> m = m.capture("instant")
    >>> tt.sleep(1)
    >>> m = m.capture("a second")
    >>> s = m.chain()
    >>> s.startswith('[start] 0.0'), 's-> [instant] 1.' in s , s.endswith('s-> [a second]')
    (True, True, True)
    """

    time: float
    name: str
    prev: "Moment"

    def __init__(self, name: str, prev: "Moment" = None) -> None:
        self.time = tt.time()
        self.name = name
        self.prev = prev

    @staticmethod
    def start():
        """capture the starting moment"""
        return Moment("start")

    def capture(self, name: str):
        """capture the named moment relative to this one"""
        return Moment(name, self)

    def elapsed(self):
        """return time in seconds since previous moment"""
        return self.time - self.prev.time

    def __str__(self):
        return (
            f" {self.elapsed():.3f}s-> [{self.name}]"
            if self.prev is not None
            else f"[{self.name}]"
        )

    def chain(self):
        return str(self) if self.prev is None else self.prev.chain() + str(self)


class PeriodicTask:
    freq:int
    logic:Callable[[],Any]
    last_run:float = None

    def __init__(self, freq:int, logic:Callable[[],Any]) -> None:
        self.freq = freq
        self.logic = logic

    def is_due(self):
        return self.last_run is None or stime.time() - self.last_run > self.freq


def gcd_pair(a, b):
    """
    >>> gcd_pair(4, 6)
    2
    >>> gcd_pair(6*15, 6*7)
    6
    >>> gcd_pair(6,35)
    1
    """
    return abs(a) if b == 0 else gcd_pair(b, a % b)


def gcd(*nn):
    """
        >>> gcd(4)
        4
        >>> gcd(4, 6)
        2
        >>> gcd(6*15, 6*7)
        6
        >>> gcd(6,35)
        1
        >>> gcd(6*15, 6*7, 6*5)
        6
        >>> gcd(6*15, 6*7, 10)
        2
        >>> gcd(6*15, 6*7, 35)
        1
        >>> gcd()
        Traceback (most recent call last):
        ...
        IndexError: tuple index out of range
    """
    r = nn[0]
    for i in range(1, len(nn)):
        r = gcd_pair(r, nn[i])
    return r

def _collect_nothing(n:str,x:Any): pass # pragma: no cover

async def run_all(*tasks: PeriodicTask, shutdown_event=None, collect_results:Callable[[str,Any],None]=_collect_nothing):
    if shutdown_event is None:
        shutdown_event = asyncio.Event()
    if len(tasks) == 0:
        log.warning('No tasks to run')
        return
    tick = gcd(*[t.freq for t in tasks])
    loop = asyncio.get_running_loop()
    while shutdown_event.is_set() is False:
        start = stime.time()
        for t in tasks:
            if t.is_due():
                t.last_run = start

                try:
                    if inspect.iscoroutinefunction(t.logic) :
                        r = await t.logic()
                    else:
                        r = await loop.run_in_executor(None, t.logic)
                except :
                    r = sys.exc_info()
                collect_results(t.logic.__name__, r)
        elapsed = stime.time() - start
        await asyncio.sleep(tick - elapsed if elapsed < tick else 0)


def adjust_as_of_date(as_of_date: date) -> date:
    """
    >>> adjust_as_of_date(None) == date.today()
    True
    >>> adjust_as_of_date(date(2021, 1, 1)) == date(2021, 1, 1)
    True
    """
    return date.today() if as_of_date is None else as_of_date
