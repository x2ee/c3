import base64
from inspect import isclass, isfunction, ismodule, iscoroutinefunction
import json
import sys
from types import ModuleType
from typing import Any, Dict, Optional, Union

import logging

from pydantic import BaseModel

log = logging.getLogger(__name__)


class GlobalRef:
    """
    >>> ref = GlobalRef('x2.c3:GlobalRef')
    >>> ref
    GlobalRef('x2.c3:GlobalRef')
    >>> ref.get_instance().__name__
    'GlobalRef'
    >>> ref.is_module()
    False
    >>> ref.get_module().__name__
    'x2.c3'
    >>> grgr = GlobalRef(GlobalRef)
    >>> grgr
    GlobalRef('x2.c3:GlobalRef')
    >>> grgr.get_instance()
    <class 'x2.c3.GlobalRef'>
    >>> grgr.is_class()
    True
    >>> grgr.is_function()
    False
    >>> grgr.is_module()
    False
    >>> uref = GlobalRef('x2.c3:')
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'x2.c3'
    >>> uref = GlobalRef('x2.c3')
    >>> uref.is_module()
    True
    >>> uref = GlobalRef(uref)
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'x2.c3'
    >>> uref = GlobalRef(uref.get_module())
    >>> uref.is_module()
    True
    >>> uref.get_module().__name__
    'x2.c3'
    """
    module: str
    name: str

    def __init__(self, s: Any, item: Optional[str] = None) -> None:
        if isinstance(s, GlobalRef):
            self.module, self.name = s.module, s.name
        elif ismodule(s):
            self.module, self.name = s.__name__, ""
        elif isclass(s) or isfunction(s):
            self.module, self.name = s.__module__, s.__name__
        else:
            split = s.split(":")
            if len(split) == 1:
                if not (split[0]):
                    raise AssertionError(f"is {repr(s)} empty?")
                split.append("")
            elif len(split) != 2:
                raise AssertionError(f"too many ':' in: {repr(s)}")
            self.module, self.name = split

    def __str__(self):
        return f"{self.module}:{self.name}"

    def __repr__(self):
        return f"{self.__class__.__name__}({repr(str(self))})"

    def get_module(self) -> ModuleType:
        return __import__(self.module, fromlist=[""])

    def is_module(self) -> bool:
        return not (self.name)

    def is_class(self) -> bool:
        return not (self.is_module()) and isclass(self.get_instance())

    def is_function(self) -> bool:
        return not (self.is_module()) and isfunction(self.get_instance())

    def is_async(self)->bool:
        if self.is_module():
            return False
        if self.is_class():
            return iscoroutinefunction(self.get_instance().__call__)
        return iscoroutinefunction(self.get_instance())

    def get_instance(self) -> Any:
        if self.is_module():
            raise AssertionError(f"{repr(self)}.get_module() only")
        attr = getattr(self.get_module(), self.name)
        return attr


class Logic:

    def __init__(self, config: Dict[str, Any], default_ref:Union[str,GlobalRef]=None) -> None:
        config = dict(config)
        try:
            if default_ref is not None:
                ref = GlobalRef( config.pop("ref$", default_ref))
            else:
                ref = GlobalRef(config.pop("ref$"))
            self.async_call = ref.is_async()
            if ref.is_function():
                self.instance = None
                self.call = ref.get_instance()
                assert config == {}, f"Unexpected entries {config}"
            elif ref.is_class():
                cls = ref.get_instance()
                self.call = self.instance = cls(config)
            else:
                raise AssertionError(f"Invalid logic {ref} in config {config}")
        except:
            log.error(f"Error in {config}")
            raise


def get_module(name:str)->ModuleType:
    """
    >>> type(get_module('x2.c3'))
    <class 'module'>
    >>> get_module('x2.c99')
    Traceback (most recent call last):
    ...
    ModuleNotFoundError: No module named 'x2.c99'
    """
    if name in sys.modules:
        return sys.modules[name]
    return __import__(name, fromlist=[""])


def str_or_none(s:Any)->Optional[str]:
    return str(s) if s is not None else None


class JsonBase(BaseModel):
    @classmethod
    def dump_schema(cls)->str:
        return json.dumps(cls.model_json_schema())

    @classmethod
    def from_json(cls, json_str):
        return cls.model_validate_json(json_str)
    
    @classmethod
    def from_base64(cls, base64_str):
        return cls.from_json(base64.b64decode(base64_str).decode())
    
    def to_base64(self)->bytes:
        return base64.b64encode(self.model_dump_json().encode('utf8'))
