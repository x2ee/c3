from typing import Any, Callable, Dict
from x2.c3 import GlobalRef, Logic
import asyncio
import pytest


def s2():
    return 2


async def a1():
    return 1

class A2:

    def __init__(self, config: Dict[str, Any]):
        config = dict(config)
        self.a = config.pop("a", 2)
        assert config == {}, f"not supported keys in config: {config}"

    async def __call__(self, *args):
        return self.a + args[0]  


class M3:
    def __init__(self, config:Dict[str,Any]):
        config = dict(config)
        self.i = config.pop("i", 3)
        assert config == {}, f"not supported keys in config: {config}"

    def __call__(self, *args):
        return self.i * args[0]


def test_coros():
    for gref in [GlobalRef("asyncio"), GlobalRef(asyncio)]:
        assert not gref.is_async()
        assert not gref.is_function()
        assert not gref.is_class()
        assert gref.is_module()
        assert gref.get_module() == asyncio

    gref = GlobalRef(a1)
    assert gref.is_async()
    assert gref.is_function()
    assert not gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == a1

    gref = GlobalRef(s2)
    assert not gref.is_async()
    assert gref.is_function()
    assert not gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == s2

    gref = GlobalRef(A2)
    assert gref.is_async()
    assert not gref.is_function()
    assert gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == A2

    gref = GlobalRef(M3)
    assert not gref.is_async()
    assert not gref.is_function()
    assert gref.is_class()
    assert not gref.is_module()
    assert gref.get_instance() == M3

def assert_ae(call:Callable[[],Any], expected_to_start_with:str, expected_exception=AssertionError):
    try:
        call()
    except Exception as e:
        print(repr(str(e)))
        assert isinstance(e, expected_exception)
        assert str(e).startswith(expected_to_start_with)
    else:
        assert False, f"Expected AssertionError({expected_to_start_with!r}) not raised"

def test_logic():
    assert Logic({"ref$": "x2.c3.tests.gref_tests:s2"}).call() == 2
    assert_ae(
        lambda: Logic({"ref$": "x2.c3.tests.gref_tests:s2", "a": 3}), 
        "Unexpected entries {'a': 3}"
    )
    assert asyncio.run(Logic({"ref$": "x2.c3.tests.gref_tests:A2"}).call(2)) == 4
    assert asyncio.run(Logic({"ref$": "x2.c3.tests.gref_tests:A2", "a":5}).call(2)) == 7
    assert_ae(
        lambda: Logic({"ref$": "x2.c3.tests.gref_tests:A2", "x": 5}),
        "not supported keys in config: {'x': 5}",
    )
    assert Logic({"ref$": "x2.c3.tests.gref_tests:M3"}).call(2) == 6
    assert Logic({"ref$": "x2.c3.tests.gref_tests:M3", "i": 5}).call(2) == 10
    assert_ae(
        lambda: Logic({"ref$": "x2.c3.tests.gref_tests:A2", "x": 4}),
        "not supported keys in config: {'x': 4}",
    )
    assert Logic({"i": 5}, "x2.c3.tests.gref_tests:M3").call(2) == 10
    assert Logic({"ref$": "x2.c3.tests.gref_tests:M3", "i": 5}, "x2.c3.tests.gref_tests:M4").call(2) == 10
    assert_ae(
        lambda: Logic(
            {"ref$": "x2.c3.tests.gref_tests:M4", "i": 5}, "x2.c3.tests.gref_tests:M3"
        ).call(2),
        "module 'x2.c3.tests.gref_tests' has no attribute 'M4'",
        AttributeError,
    )
