from typing import cast
from x2.c3.ctx import config, Config
from x2.c3.db import SQLiteDbMap
from x2.c3.dnode import DNodeTree, DataNode
import pytest

@pytest.mark.debug
def test_load_config():
    cfg = Config(module=__name__, set_in_ctx=True)
    assert config.get() == cfg
    assert isinstance(cfg.dbm, SQLiteDbMap)
    assert isinstance(cfg.data_tree, DNodeTree)
    # assert cfg.data_tree.get("asset/yf/info")
    # assert cast(DataNode, cfg.data_tree.get("asset/yf/info")).compute is not None

