import asyncio
import time

import pytest
from x2.c3.periodic import PeriodicTask, run_all

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
