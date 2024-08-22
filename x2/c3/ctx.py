import json
from contextvars import ContextVar

import sys
from typing import Any, Dict, Union
from types import ModuleType
from pathlib import Path
from x2.c3.db import SQLiteDbMap
from x2.c3.dnode import DNodeTree, DataNode
from x2.c3.dpath import DataPath

class Config:

    def __init__(
        self,
        db_root: Union[Path, str] = None,
        module:Union[str, ModuleType]= None,
        cfg_path: Union[Path, str] = None,
        set_in_ctx: bool = False,
    ) -> None:
        if db_root is None:
            db_root = "data"
        db_root = Path(db_root).absolute()
        self.dbm = SQLiteDbMap(db_root, auto_create=True)

        if module is not None:
            print(module)
            if isinstance(module, str):
                if module in sys.modules:
                    module = sys.modules[module]
                else:
                    module = __import__(module)
            cfg_path = Path(module.__file__).parent / "dnodes.json"
            print(module, module.__file__, cfg_path, cfg_path.exists())
        else:
            assert cfg_path is not None
            cfg_path = Path(cfg_path)
        with cfg_path.open() as f:
            cfg_dict = json.load(f)
            self.data_tree = DNodeTree(cfg_dict.pop("dnodes"))
            assert cfg_dict == {}, f"Unexpected entries {config}"
            if set_in_ctx:
                config.set(self)

    def dn(self, path: DataPath) -> DataNode:
        dn = self.data_tree.get(path)
        assert isinstance(dn, DataNode)
        return dn


config = ContextVar[Config]("config")
