from datetime import date, datetime
import json as _json
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Tuple, Type, Union

import pandas as pd
import numpy as np

from x2.c3 import GlobalRef
from x2.c3.periodic import Interval

def _identity(x): return x

def coerce_numpy_to_python(obj: Any, default: Callable[[Any],Any]=_identity) -> Any:
        if isinstance( obj, ( np.int_, np.intc, np.intp, np.int8, 
                            np.int16, np.int32, np.int64, np.uint8, 
                            np.uint16, np.uint32, np.uint64,) ):
            return int(obj)
        elif isinstance(obj, (np.float16, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, (np.ndarray,)):
            return obj.tolist()
        elif isinstance(obj, (np.bool_)):
            return bool(obj)
        elif isinstance(obj, (np.void)):
            return None
        return default(obj)

class NumpyEncoder(_json.JSONEncoder):
    """Custom encoder for numpy data types"""

    def default(self, obj):
        return coerce_numpy_to_python(obj,lambda o:_json.JSONEncoder.default(self, o))

json_loads = lambda s: _json.loads(s)
json_dumps = lambda j: _json.dumps(j, cls=NumpyEncoder)

def from_json(json_obj: Any) -> Any:
    """ 
    resolve type_ref$ in the nested json object"""
    if isinstance(json_obj, dict):
        json_dict = json_obj.copy()
        if "type_ref$" in json_obj:
            type_ref = GlobalRef(json_dict.pop("type_ref$"))
            type_ = type_ref.get_instance()
            return convert_to_type(json_dict, type_)
        else:
            return {k: from_json(v) for k, v in json_dict.items()}
    elif isinstance(json_obj, (tuple,list)):
        return [from_json(v) for v in json_obj]
    else:
        return json_obj

def to_json(obj: Any) -> Any:
    if obj is None:
        return None
    type_ = type(obj)
    if type_ in KNOWN_TYPES_BY_TYPE:
        return KNOWN_TYPES_BY_TYPE[type_].to_json(obj)
    if isinstance(obj, dict):
        return {k: to_json(v) for k, v in obj.items()}
    elif isinstance(obj, (tuple,list)):
        return [to_json(v) for v in obj]
    raise AssertionError(f"Cannot convert {obj} of {type_} to json")

def df_to_json(df: pd.DataFrame) -> Dict[str, Any]:
    series_dict = df.to_dict(orient="series")
    return {
        "type_ref$": "pandas.core.frame:DataFrame",
        "series": {
            k: dict(dtype=s.dtype.name, data=list(map(coerce_numpy_to_python, s.array)))
            for k, s in series_dict.items()
        },
    }

def json_to_df(json: Dict[str, Any]) -> pd.DataFrame:
    return pd.DataFrame(
        {k: pd.Series(np.array(v["data"]), dtype=np.dtype(v["dtype"])) for k, v in json["series"].items()}
    )

def df_from_str(raw: str)->pd.DataFrame:
    return json_to_df(json_loads(raw))

def df_to_str(df) -> str:
    return json_dumps(df_to_json(df))


class KnownType:
    @classmethod
    def ensure(cls, type_:Any)->"KnownType":
        if isinstance(type_, KnownType):
            return type_
        return resolve_type(type_)

    def __init__(self, name:str, type_:type, json_type=None):
        self.name = name
        self.type = type_
        self.json_type = json_type

    def to_type_safe(self, s:str)->Any:
        if s is None:
            return None
        else:
            return self.to_type(s)
        
    def to_json(self, value:Any)->Any:
        if value is None or self.json_type is None:
            return value
        return convert_to_type(value, self.json_type)

    def to_str(self, value:Any)->str:
        return TYPES_CONVERSIONS.convert(value, str)
    
    def to_type(self, s:str)->Any:
        return TYPES_CONVERSIONS.convert(s, self.type)

    def __repr__(self):
        return f"KnownType({self.name!r}, {self.type!r})"

KNOWN_TYPES = {
    t.name: t
    for t in [
        KnownType("int", int),
        KnownType("float", float),
        KnownType("str", str),
        KnownType("bool", bool),
        KnownType("date", date, json_type=str),
        KnownType("datetime", datetime, json_type=str),
        KnownType("path", Path, json_type=str),
        KnownType("interval", Interval, json_type=str),
        KnownType("dataframe", pd.DataFrame, json_type=dict),
        KnownType("blob", bytes),
    ]
}

KNOWN_TYPES_BY_TYPE = { t.type: t for t in KNOWN_TYPES.values() }


def resolve_type(type_: Union[type, str]) -> KnownType:
    """
    >>> resolve_type(int)
    KnownType('int', <class 'int'>)
    >>> resolve_type("int")
    KnownType('int', <class 'int'>)
    >>> resolve_type("zzz")
    Traceback (most recent call last):
    ...
    ValueError: Unknown type zzz
    >>> resolve_type(dict)
    Traceback (most recent call last):
    ...
    ValueError: Unknown type <class 'dict'>
    >>> resolve_type(3)
    Traceback (most recent call last):
    ...
    ValueError: Unknown type 3
    """
    if isinstance(type_, str):
        type_ = type_.lower()
        if type_ in KNOWN_TYPES:
            return KNOWN_TYPES[type_]
    elif isinstance(type_, type):
        if type_ in KNOWN_TYPES_BY_TYPE:
            return KNOWN_TYPES_BY_TYPE[type_]
    raise ValueError(f"Unknown type {type_}")


class TypeConversionMatrix:
    mapping: Dict[Tuple[Type,Type], Callable[[Any],Any]]

    def __init__(self, *mappings:Tuple[Type,Type,Callable[[Any],Any]]):
        self.mapping = {}
        for m in mappings:
            self.add(*m)

    def add(self, from_:Type, to:Type, fn:Callable[[Any],Any])->"TypeConversionMatrix":
        self.mapping[(from_, to)] = fn
        return self

    def mappings(self)->Iterable[Tuple[Type,Type,Callable[[Any],Any]]]:
        for k, v in self.mapping.items():
            yield k[0], k[1], v

    def convert(
        self,
        value: Any,
        to: Type
    ) -> Any:
        assert to is not None, f"Cannot convert to None"
        if value is None:
            return None
        from_: Type = type(value)
        if from_ == to:
            return value
        k = (from_, to)
        if k in self.mapping:
            fn = self.mapping[k]
            if fn is None:
                raise ValueError(
                    f"Cannot convert from {from_} to {to}, `None` conversion defined"
                )
            return fn(value)
        return to(value)

TYPES_CONVERSIONS = TypeConversionMatrix(
    (str, date, date.fromisoformat),
    (date, str, lambda x: x.isoformat()),
    (str, datetime, datetime.fromisoformat),
    (datetime, str, lambda x: x.isoformat()),
    (str, Interval, Interval.from_string),
    (pd.DataFrame, dict, df_to_json),
    (dict, pd.DataFrame, json_to_df),
    (pd.DataFrame, str, df_to_str),
    (str, pd.DataFrame, df_from_str),
    (dict, str, json_dumps),
    (str, dict, json_loads),
)

def convert_to_type(value:Any, to:Type)->Any:
    return TYPES_CONVERSIONS.convert(value, to)


class HasDefault:
    """
    >>> HasDefault.from_json(None) is None
    True
    >>> HasDefault.from_json([None]).default is None
    True
    >>> HasDefault.from_json(1).default
    1
    >>> HasDefault.from_json(None) is None
    True
    >>> HasDefault.from_json([None])
    HasDefault(None)
    >>> HasDefault.from_json(1)
    HasDefault(1)
    >>> HasDefault.from_json(1).to_json()
    1
    >>> HasDefault.from_json([None]).to_json()
    [None]
    """
    default: Any

    @staticmethod
    def from_json(json:Any, type_:KnownType = None)->"HasDefault":
        if json is None:
            return None
        if json == [None]:
            return HasDefault(None)
        return HasDefault(json if type_ is None else type_.to_type_safe(json))

    def __init__(self, default):
        self.default = default

    def __repr__(self):
        return f"HasDefault({self.default!r})"

    def to_json(self)->Any:
        return self.default if self.default is not None else [None] 

class ArgField:
    name: str
    type: KnownType
    default: HasDefault
    is_key: bool 

    @staticmethod
    def from_dict(config:Dict[str,Any])->"ArgField":
        config = config.copy()
        name = config.pop('name')
        type_ = resolve_type(config.pop('type'))
        default = HasDefault.from_json(config.pop('default', None), type_)
        is_key = config.pop('is_key', False)
        assert config == {}, f"Unexpected entries {config}"
        return ArgField(name, type_, default=default, is_key=is_key)

    def __init__(self, name:str, type_:Union[KnownType,str,Type], default:HasDefault=None, is_key:bool=False):
        self.name = name
        self.type = KnownType.ensure(type_)
        self.default = default
        self.is_key = is_key

    def __copy__(self):
        return ArgField(self.name, self.type, self.default, self.is_key)

    def __repr__(self):
        return f"ArgField({self.name!r}, {self.type!r}, {self.default!r}, {self.is_key!r})"

    def to_dict(self)->Dict[str,Any]:
        r: Dict[str, Any] = {"name": self.name, "type": self.type.name}
        if self.default is not None:
            r['default'] = self.default.to_json()
        if self.is_key:
            r['is_key'] = True
        return r

    @staticmethod
    def build_field_dict(fields:Iterable["ArgField"])->Dict[str, "ArgField"]:
        return {f.name: f for f in fields}

class Table:
    name: str
    fields: Dict[str, ArgField]

    @staticmethod
    def from_dict(config:Dict[str,Any])->"Table":
        config = config.copy()
        name = config.pop('name')
        fields = ArgField.build_field_dict(map(ArgField.from_dict, config.pop('fields')))
        assert config == {}, f"Unexpected entries {config}"
        return Table(name, fields)

    def __init__(self, name:str, fields:Union[List[ArgField],Dict[str,ArgField]]):
        self.name = name
        self.fields = fields if isinstance(fields, dict) else ArgField.build_field_dict(fields)

    def __repr__(self):
        return f"Table({self.name!r}, {self.fields!r})"
