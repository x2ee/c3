"""
HostWorkerModel (HWM)
"""

from base64 import b64decode, b64encode
import random
import time
from typing import Union


def encode_base64(bb: bytes) -> str:
    return b64encode(bb).decode()


def ensure_bytes(input: Union[str, bytes]) -> bytes:
    if isinstance(input, str):
        input = b64decode(input)
    return input

MIN_NON_PRIVILEGED = 1025
MIN_NON_RESERVED = 49152
MAX_PORT = 65535

random.seed(time.time())


def rand_uint(start: int):
    return random.randint(start, MAX_PORT)


def random_port(only_non_reserved=False):
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
    if (not only_non_reserved) and random.randint(1, 3) == 3:
        return rand_uint(MIN_NON_PRIVILEGED)
    return rand_uint(MIN_NON_RESERVED)
