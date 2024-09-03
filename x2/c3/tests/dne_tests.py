import x2.c3.event as e
import base64, sys, json, pydantic
import pytest

from x2.c3.periodic import Interval

def test_cache_params():
    false_null = '{"force":false,"interval":null}'
    true_2w = '{"force":true,"interval":"2W"}'
    try:
        e.CacheParams.from_json('{"force":true,"interval":"2x"}')
        assert False
    except pydantic.ValidationError as ve:
        errs = ve.errors()
        assert 1 == len(errs)
        val_err = errs[0]['ctx']['error']
        assert type(val_err) == ValueError
        assert repr(val_err) == "ValueError('Invalid frequency string', '2x')"
    try:
        e.CacheParams(force=True, interval="2x")
        assert False
    except pydantic.ValidationError as ve:
        errs = ve.errors()
        assert 1 == len(errs)
        val_err = errs[0]['ctx']['error']
        assert type(val_err) == ValueError
        assert repr(val_err) == "ValueError('Invalid frequency string', '2x')"

    assert false_null == e.CacheParams(force=False, interval=None).model_dump_json()
    assert true_2w == e.CacheParams(force=True, interval=Interval.from_string("2w")).model_dump_json()
    assert true_2w == e.CacheParams(force=True, interval="2w").model_dump_json()
    
    assert e.CacheParams.from_json(false_null).model_dump_json() == false_null
    assert e.CacheParams.from_json(true_2w).model_dump_json() == true_2w

    assert e.CacheParams.from_base64(e.CacheParams.from_json(false_null).to_base64()).model_dump_json() == false_null
    assert e.CacheParams.from_base64(e.CacheParams.from_json(true_2w).to_base64()).model_dump_json() == true_2w
    schema = (
        '{"properties": {'
            '"force": {"default": false, "title": "force cache refresh", "type": "boolean"}, '
            '"interval": {"anyOf": [{"type": "string"}, {"type": "null"}], "default": null, "title": "time interval for cache expiration"}}, '
        '"title": "Cache expiration parameters", "type": "object"}'
    )
    actual = e.CacheParams.dump_schema()
    print(actual)
    assert actual == schema
    # assert False

