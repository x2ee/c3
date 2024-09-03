# WEP Stands for Worker End Point

from datetime import datetime
import random
import time
from typing import List

from x2.c3 import JsonBase

MIN_NON_PRIVILEGED = 1025
MIN_NON_RESERVED = 49152
MAX_UINT = 65535

random.seed(time.time())

def rand_uint(start:int):
    return random.randint(start, MAX_UINT)

def random_port(only_non_reserved = False):
    """
    Return a random non privileged port number, giving by default higher probability to 
    non-reserved port numbers (>=49152).

    >>> defaults = [ MIN_NON_RESERVED <= random_port() for _ in range(1000)]
    >>> non_reserved = [ MIN_NON_RESERVED <= random_port(True) for _ in range(1000)]
    >>> sum(non_reserved)/len(non_reserved)
    1.0
    >>> sum(defaults)/len(defaults) > .7 # usually in range .75 - .78
    True
    """
    if (not only_non_reserved) and random.randint(1,3) == 3:
        return rand_uint(MIN_NON_PRIVILEGED)
    return rand_uint(MIN_NON_RESERVED)

class WorkerEndPoint(JsonBase):
    hostname:str
    pid:int
    bind_address:str
    port:int
    public_key:str
    updated: datetime
    tags: List[str]

class WorkerInitiationRequest(JsonBase):
    host: WorkerEndPoint

class WorkerBindResponse(JsonBase):
    worker: WorkerEndPoint
    self_sig: str

class WorkerListedAcknowledge(JsonBase):
    host: WorkerEndPoint
    worker: WorkerEndPoint
    self_sig: str
    listed: datetime
    host_sig: str


    






