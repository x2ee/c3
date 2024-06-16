from x2.c3 import GlobalRef
import asyncio
import pytest

async def a1():
    return 1

class A2:
    async def __call__(self, *args):
        return 2*args[0]  


def test_coros():
    gref = GlobalRef(a1)
    assert gref.is_async()
    assert gref.is_function()
    assert not gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == a1

    gref = GlobalRef(A2)
    assert gref.is_async()
    assert not gref.is_function()
    assert gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == A2
