import asyncio
from datetime import datetime, timedelta, timezone
import time
import pytest
from x2.c3.periodic import EPOCH_ZERO, PeriodicTask, dt_to_bytes, run_all, dt_from_bytes

@pytest.mark.slow
def test_periodic():
    results = []
    beginning = time.time()
    def dump_results(*t):
        a = (time.time()-beginning, *t)
        print(a)
        results.append(a)

    async def run_all_tasks():
        def build_fn(return_value:bool, async_fn:bool, sleep_time:float, ):
            prefix: str = "a" if async_fn else ""
            rx = 'r' if return_value else 'x'
            n = f"{prefix}sync_fn_{rx}_{sleep_time}"

            def fn_end(start:float):
                t = time.time()
                dump_results("fn_end", n, t - start)
                if return_value:
                    return sleep_time
                else:
                    raise ValueError(sleep_time)
            if async_fn:
                async def fn():
                    start = time.time()
                    await asyncio.sleep(sleep_time)
                    return fn_end(start)
            else:
                def fn():
                    start = time.time()
                    time.sleep(sleep_time)
                    return fn_end(start)
            fn.__name__ = n
            return fn

        shutdown_event = asyncio.Event()

        f_tasks = asyncio.create_task(run_all(
            *[PeriodicTask(*v) for v in [
                (6, build_fn(True, True, 1)),
                (8, build_fn(True, False, 2)),
                (4, build_fn(False, True, 1)),
                (7, build_fn(False, False, 1))
            ]],
            shutdown_event=shutdown_event,
            collect_results=lambda n, r: dump_results("result", n, r)
        ))                

        await asyncio.sleep(20)
        shutdown_event.set()
        await f_tasks
        return results

    asyncio.run(run_all_tasks())
    def r2s(t):
        if t[1] == 'fn_end':
            t = (int(t[0]), t[1], t[2], int(t[3]))
        elif t[1] == 'result':
            v = t[3]
            if isinstance(v, tuple):
                v = v[:2]
            t = (int(t[0]), t[1], t[2], v)
        return str(t)
    str_results = tuple(map(r2s, results))
    for s in str_results:
        print(f"{s!r},")
    assert str_results == (
        "(1, 'fn_end', 'async_fn_r_1', 1)",
        "(1, 'result', 'async_fn_r_1', 1)",
        "(3, 'fn_end', 'sync_fn_r_2', 2)",
        "(3, 'result', 'sync_fn_r_2', 2)",
        "(4, 'fn_end', 'async_fn_x_1', 1)",
        "(4, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(5, 'fn_end', 'sync_fn_x_1', 1)",
        "(5, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(6, 'fn_end', 'async_fn_x_1', 1)",
        "(6, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(7, 'fn_end', 'async_fn_r_1', 1)",
        "(7, 'result', 'async_fn_r_1', 1)",
        "(8, 'fn_end', 'sync_fn_x_1', 1)",
        "(8, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(10, 'fn_end', 'sync_fn_r_2', 2)",
        "(10, 'result', 'sync_fn_r_2', 2)",
        "(11, 'fn_end', 'async_fn_x_1', 1)",
        "(11, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(13, 'fn_end', 'async_fn_r_1', 1)",
        "(13, 'result', 'async_fn_r_1', 1)",
        "(14, 'fn_end', 'async_fn_x_1', 1)",
        "(14, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(15, 'fn_end', 'sync_fn_x_1', 1)",
        "(15, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(18, 'fn_end', 'sync_fn_r_2', 2)",
        "(18, 'result', 'sync_fn_r_2', 2)",
        "(19, 'fn_end', 'async_fn_x_1', 1)",
        "(19, 'result', 'async_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
        "(20, 'fn_end', 'sync_fn_x_1', 1)",
        "(20, 'result', 'sync_fn_x_1', (<class 'ValueError'>, ValueError(1)))",
    )
    asyncio.run(run_all())
    # assert False

def test_max_values_for_datetime_serialized():
    dt_max = datetime.max
    dt_min = datetime.min
    dt_max_utc = datetime.max.replace(tzinfo=timezone.utc)
    dt_min_utc = datetime.min.replace(tzinfo=timezone.utc)

    extremes = (
        (dt_max_utc - EPOCH_ZERO).total_seconds(), 
        (EPOCH_ZERO - dt_min_utc).total_seconds()
    )
    max_value_to_store = int(max(extremes)*1000000)
    how_many_bytes = (max_value_to_store).bit_length() // 8 + 1
    print(f"{how_many_bytes=}")
    round_trip = lambda dt: dt_from_bytes(dt_to_bytes(dt))

    def round_trip_n(n,dt,dt_expected,step):
        h = f" {n=}\n"
        s = ""
        all = True
        for i in range(n):
            try: 
                v = round_trip(dt) 
                c = (v == dt_expected)
            except OverflowError:
                v,c = None, None
            s += f"{(c, v, dt, dt_expected)}\n"
            if not c:
                all = False
            dt = dt + timedelta(microseconds=step)
            dt_expected = dt_expected + timedelta(microseconds=step)
        return all, f" {n=} {all=}\n{s}"
    
    all, msg = round_trip_n(10, dt_max, dt_max_utc, -1)
    assert all, msg
    all, msg = round_trip_n(100, dt_max, dt_max_utc, -91)
    assert all, msg
    all, msg = round_trip_n(100, dt_max_utc, dt_max_utc, -791)
    assert all, msg
    all, msg = round_trip_n(10, dt_min, dt_min_utc, 1)
    assert all, msg
    all, msg = round_trip_n(100, dt_min, dt_min_utc, 91)
    assert all, msg
    all, msg = round_trip_n(100, dt_min_utc, dt_min_utc, 791)
    assert all, msg

    # print(f"{dt_max=} {round_trip_n(100, dt_max, dt_max_utc, -10)[1]}")
    # print(f"{dt_min=} {round_trip_n(100, dt_min, dt_min_utc, 10)[1]}")
    # assert False
