import asyncio
from datetime import date, datetime
import logging.handlers
from croniter import croniter
from typing import Any, Dict, Generator, List, Optional, Tuple, Union, cast
from x2.c3 import Logic
from x2.c3.types import ArgField
from x2.c3.dpath import DataPath 
from x2.c3.event import DnEvent
from x2.c3.periodic import Interval, stamp_time, adjust_as_of_date
import pandas as pd

import logging
log = logging.getLogger(__name__)
# logging.basicConfig(format="%(asctime)s %(threadName)s:%(taskName)s - %(message)s")


class DNode:
    def __init__(self, tree:"DNodeTree", path:DataPath) -> None:
        self.tree = tree
        self.path = path
        if self.tree != self:
            pp = path.parents()
            if len(pp) > 1:
                for i in range(1,len(pp)):
                    parent_node = cast(DirNode, tree[pp[i-1]])
                    child_node = tree.get(pp[i])
                    if child_node is None:
                        parent_node.add(DirNode(tree, pp[i]))
                cast(DirNode, tree[pp[-1]]).add(self)


class DirNode(DNode):
    def __init__(self, tree:"DNodeTree", path:DataPath) -> None:
        self.defaults = None
        self.children: Dict[str, DNode] = {}
        super().__init__(tree, path)

    def set_config(self, config:Dict[str, Any])->None:
        config = config.copy()
        self.defaults = config.pop('defaults', None)
        if self.defaults:
            _validate_data_node_config(self.defaults)
        assert config == {}, f"Unexpected entries {config}"

    def add(self, node:DNode)->None:
        self.children[node.path.name()] = node
        self.tree.all_nodes[node.path] = node

    def iterate_all(self)->Generator[DNode, None, None ]:
        for c in self.children.values():
            yield c
            if isinstance(c, DirNode):
                yield from c.iterate_all()


class DNodeTree(DirNode):
    def __init__(self, input_config: Dict[str, Any] = {}) -> None:
        path = DataPath.ensure_path('')
        self.input_config = dict(input_config)
        self.all_nodes:Dict[DataPath, DNode] = {path: self}
        super().__init__(self, path)

        def parse_dnodes(root: "DNodeTree", cur: DataPath, config: Dict[str, Any]):
            for k,v in config.items():
                k_path = cur / k
                assert isinstance(v, dict)
                if 'compute' in v:
                    DataNode(self, k_path, v)
                else:
                    copy_dict = dict(v)
                    children = copy_dict.pop("children", None)
                    if k_path in self.all_nodes:
                        dir_node = cast(DirNode, self.all_nodes[k_path])
                    else:
                        dir_node = DirNode(self, k_path)
                        self.all_nodes[k_path] = dir_node
                    dir_node.set_config(copy_dict)
                    if children:
                        parse_dnodes(self, k_path, children)

        parse_dnodes(self, self.path, input_config)

        for v in self.iterate_all():
            if isinstance(v, DataNode):
                v.init_data_node()

    def __getitem__(self, path:Union[str,DataPath])->DNode:
        return self.all_nodes[DataPath.ensure_path(path)]

    def get(self, path: Union[str, DataPath]) -> DNode:
        path = DataPath.ensure_path(path)
        return self[path] if path in self else None

    def __contains__(self, path: Union[str, DataPath]) -> bool:
        path = DataPath.ensure_path(path)
        return path in self.all_nodes

class DataNodeAware:
    node: "DataNode"

    def init_with_node(self, node: "DataNode"):
        self.node = node


class DnCache(DataNodeAware):
    
    def get(self, dne:DnEvent) -> str:
        raise NotImplementedError()
    
    def get_distinct_keys(self, as_of_date:date, interval:Interval = None) -> pd.DataFrame:
        raise NotImplementedError()


class DnState(DataNodeAware):

    def read(self, as_of_date: date, interval:Interval, *key_values) -> Tuple[date, str]:
        raise NotImplementedError()
    
    def write(self, text:str, as_of_date:date, *key_values) -> None:
        raise NotImplementedError()

    def get_distinct_keys(self, as_of_date:date, interval:Interval) -> pd.DataFrame:
        raise NotImplementedError()

class CronTask(DataNodeAware):
    def __init__(self, config:Dict[str, Any]) -> None:
        config = config.copy()
        self.name = config.pop("name")
        self._hash_id:Optional[str] = None
        self.schedule = config.pop("schedule")
        self.logic = Logic(config.pop("logic"))
        assert (
            config == {}
        ), f"Unrecognized properties in `CronTask` config {config}"

    def hash_id(self):
        if self._hash_id is None:
            self._hash_id = f"{self.node.path}#{self.name}"
        return self._hash_id

    def croniter(self, last_run:datetime):
        if last_run is None:
            last_run = stamp_time()
        return croniter(self.schedule, last_run, hash_id=self.hash_id())


class RunnerMixin:
    runner_table:str
    node: "DataNode"

    def _init_runner(self, config):
        self.runner_table = config.pop("runner_table")


class DnCron(DataNodeAware, RunnerMixin):

    def __init__(self, config: Dict[str, Any]) -> None:
        config = config.copy()
        self.tasks = [CronTask(d) for d in config.pop("tasks")]
        self._init_runner(config)
        assert config == {}, f"Unrecognized properties in compute config {config}"

    def init_with_node(self, node: "DataNode"):
        self.node = node
        for t in self.tasks:
            t.init_with_node(node)


data_node_services = ("compute", "state", "cache", "cron")

def _validate_data_node_config(config:Dict[str, Any]):
    config = dict(config)
    for service_name in data_node_services:
        if service_name in config:
            del config[service_name]
    assert config == {}, f"Unrecognized properties in data node config {config}"

class DataNode(DNode):
    def __init__(
        self, tree: "DNodeTree", path: DataPath, config: Dict[str, Any]
    ) -> None:
        super().__init__(tree, path)
        _validate_data_node_config(config)
        self.config = config

        # services
        self.compute: DnCompute = None
        self.cache: DnCache = None
        self.state: DnState = None
        self.cron: DnCron = None

    def init_data_node(self):
        for service_name in data_node_services:
            self._init_service(service_name)

    def _init_service(self, service_name:str):
        top_config = self.config.get(service_name, {})
        if top_config is None:
            service_config = None
        else:
            service_config = {}
            for p in reversed(self.path.parents()):
                dir_node = cast(DirNode, self.tree[p]) 
                if dir_node.defaults:
                    service_config.update(dir_node.defaults.get(service_name, {}))
                    break
            service_config.update(top_config)
        if service_config:
            log.debug(f"Initializing service: path={self.path}#{service_name} config={service_config}")
            v = Logic(service_config).instance
            assert v is not None
            setattr(self, service_name, v)
            v.init_with_node(self)

    def get(self, *key_values:str, as_of_date=None, interval:Interval = None, force=False):
        dne = DnEvent(
            self.path,
            key_values,
            as_of_date,
            interval=interval,
            force=force,
            arg_fields=self.arg_fields(),
        )
        if self.cache is None:
            if interval is not None:
                log.warn(f"Cannot set interval on non-cached source  get_path={self.path}")
            if force :
                log.info(f"Non-cached source is always recomputed. Setting `force` has no impact get_path={self.path}")
            return self.compute.calculate(dne)
        return self.cache.get(dne)

    def get_distinct_keys(
        self, as_of_date: date = None, interval: Interval = None
    ) -> pd.DataFrame:
        if self.cache is None:
            as_of_date = adjust_as_of_date(as_of_date)
            assert interval is not None
            return self.state.get_distinct_keys(as_of_date=as_of_date, interval=interval)
        return None

    def arg_fields(self)->List[ArgField]:
        return self.compute.args

class DnCompute(DataNodeAware, RunnerMixin):
    def __init__(self, config:Dict[str, Any]) -> None:
        config = config.copy()
        self.args = [ArgField.from_dict(d) for d in config.pop('args', [])]
        self.logic = Logic(config.pop('logic'))
        self._init_runner(config)
        assert config == {}, f'Unrecognized properties in compute config {config}'

    async def calculate(self, dne:DnEvent) -> Any:
        if self.logic.async_call:
            return await self.logic.call(dne.as_of_date, *dne.typed_values)
        else:
            return asyncio.get_event_loop().run_in_executor(
                None, lambda: self.logic.call(dne.as_of_date, *dne.typed_values)
            )

