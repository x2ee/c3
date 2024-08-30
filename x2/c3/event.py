import base64
from datetime import date, datetime
from typing import Any, List, Optional, Tuple, Union, cast
from pydantic import ConfigDict, Field
import uuid
from x2.c3 import JsonBase
from x2.c3.dpath import DataPath
from x2.c3.types import ArgField
from x2.c3.periodic import Interval, IntervalSafe, Moment, stamp_time, adjust_as_of_date

import logging
log = logging.getLogger(__name__)


class CacheParams(JsonBase):
    model_config = ConfigDict(arbitrary_types_allowed=True, title="Cache expiration parameters")

    force: bool = Field(default=False, title="force cache refresh")
    interval: IntervalSafe = Field(default=None, title="time interval for cache expiration")

    def get_interval(self)->Optional[Interval]:
        return cast(Interval, self.interval)

    
class DnEvent:

    def __init__(
        self,
        path: DataPath,
        str_values: Union[List[str], Tuple[str, ...]],
        as_of_date: date,
        cache_params:CacheParams = None, 
        arg_fields: List[ArgField] = None,
        typed_values: List[Any] = None,
        time_stamp: datetime = None,
        id: str = None,
    ) -> None:
        self.id = str(uuid.uuid4()) if id is None else id
        self.time_stamp = stamp_time() if time_stamp is None else time_stamp
        self.as_of_date = adjust_as_of_date(as_of_date)
        self.path = path
        self.str_values = str_values
        self.cache_params = cache_params
        self.stages: Moment = Moment.start()
        if arg_fields is not None and typed_values is None:
            self.resolve(arg_fields)
        else:
            self.arg_fields = arg_fields
            self.typed_values = typed_values

    def capture_stage(self, stage_name: str) -> None:
        self.stages = self.stages.capture(stage_name)
        log.debug(str(self.stages))

    def resolve(self, arg_fields: List[ArgField]) -> None:
        self.arg_fields = arg_fields
        num_of_args = len(self.arg_fields)
        if len(self.str_values) != num_of_args:
            raise ValueError(f"Expected {num_of_args} keys, got {self.str_values}")
        self.typed_values = [
            f.type.to_type_safe(self.str_values[i]) for i, f in enumerate(arg_fields)
        ]

    def get_cache_params(self, expire:Interval=None)->CacheParams:
        force = False
        if self.cache_params is not None :
            if self.cache_params.interval is not None:
                return self.cache_params
            else:
                force = self.cache_params.force
        return CacheParams(force=force, interval=expire)
        
            
    