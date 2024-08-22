import asyncio
from random import randint, seed
import time
from typing import Any, Dict
import pandas as pd

seed(time.time()) 
import logging
log = logging.getLogger(__name__)

def raise_value_error_at_3(n):
    if n == 3 and randint(0, 2) != 0:
        raise ValueError("n is 3") 

class S1:
    def __init__(self, config:Dict[str, Any]):
        log.info(f"config: {config}")

    def __call__(self, n:int)->pd.DataFrame:
        log.info(f"n: {n}")
        time.sleep(n)
        raise_value_error_at_3(n)
        return {"n": n}


class A1:
    def __init__(self, config:Dict[str, Any]):
        log.info(f"config: {config}")
    
    async def __call__(self, prefix:str, n:int)->Dict[str, Any]:
        log.info(f"prefix: {prefix}, n: {n}")
        await asyncio.sleep(n)
        raise_value_error_at_3(n)
        return {"n": n}


def s2(prefix:str, n:int)->pd.DataFrame:
    log.info(f"prefix: {prefix}, n: {n}")
    time.sleep(n)
    raise_value_error_at_3(n)
    return {"n": n}

async def a2(n:int)->Dict[str, Any]:
    log.info(f"n: {n}")
    await asyncio.sleep(n)
    raise_value_error_at_3(n)
    return {"n": n}
