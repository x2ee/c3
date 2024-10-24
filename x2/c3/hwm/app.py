from pathlib import Path
import sys, time

from typing import Any, Dict, Union
import x2.c3.ctx as ctx
from x2.c3.db import SQLiteDbMap
from x2.c3.dnode import DNodeTree, DataNode, DataPath
from x2.c3.periodic import Interval
import logging
log = logging.getLogger(__name__)


def main(args=sys.argv[1:]):
    ctx.Config(module=args[0], set_in_ctx=True)
    args = list(args[1:])
    switches = {'force': False}
    interval = None

    i = 0
    for arg in list(args):
        if arg.startswith('-'):
            arg = arg[1:]
            if arg in switches:
                switches[arg] = True
                del args[i]
                continue
            if Interval.matcher(arg):
                interval = Interval.from_string(arg)
                del args[i]
                continue
        i += 1

    path, *others = args

    start = time.time()
    dn = ctx.config.get().dn(path)
    result = dn.get(*others, interval=interval, force=switches["force"])
    print(result)
    log.info("Elapsed:", time.time()-start)


if __name__ == '__main__':
    main()
