# Q: Please write connection pool class for sqlite,
# with `connection()` method that uses context manager
# to make sure that connection object returns to the pool
# after use.


from datetime import date, datetime
from enum import Enum
from pathlib import Path
import sqlite3, time
from contextlib import contextmanager
from typing import Any, ClassVar, Dict, List, Optional, Tuple, Union, cast
from copy import copy

import pandas as pd

from x2.c3.dpath import DataPath 
from x2.c3.event import DnEvent
from x2.c3.types import KNOWN_TYPES, ArgField, KnownType, Table, json_loads, json_dumps, to_json, from_json
from x2.c3.dnode import DataNode, DnCache, DnState
from x2.c3.periodic import Interval
import x2.c3.ctx as ctx

import logging
log = logging.getLogger(__name__)

class SQLiteDbMap:
    def __init__(self, root:Union[Path,str], auto_create=False, db_names:List[str]=[]):
        self.auto_create = auto_create
        self.root = Path(root)        
        self.map:Dict[str, SQLiteDb] = {}
        for n in db_names:
            self.add_with_the_same_db_name(n)

    def keys(self):
        return self.map.keys()
    
    def __getitem__(self, name:str)->"SQLiteDb":
        if self.auto_create and name not in self.map:
            self.add_with_the_same_db_name(name)
        return self.map[name]

    def add_with_the_same_db_name(self, n:str):
        self.add(n, self.root / f"{n}.db")

    def add(self, name:str, db_file: Union[str,Path])->"SQLiteDbMap":
        assert name not in self.map
        self.map[name] = SQLiteDb(db_file)
        return self

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        for db in self.map.values():
            db.close()


class SQLiteDb:
    def __init__(self, db_file:Union[str,Path]):
        if db_file != ":memory:":
            db_file= str(Path(db_file).absolute())
        self.database = db_file
        self.conn:Optional[List[sqlite3.Connection]]= None

    @contextmanager
    def connection(self, max_wait=1.):
        if self.conn is None:
            self.conn = [sqlite3.connect(self.database, check_same_thread=False)]
        while True:
            try:
                connection = self.conn.pop()
                break
            except IndexError:
                if max_wait <= 0:
                    raise ValueError('Connection pool exhausted')   
                time.sleep(.1)
                max_wait -= .1
        try:
            yield connection
            connection.commit()
        except:
            connection.rollback()
            raise
        finally:
            self.conn.append(connection)

    def close(self):
        """ try to close no matter what """
        try:
            self.conn[0].close()
            self.conn = None 
        except: pass #pragma: no cover


def exec_sql(conn, sql:str, *args):
    log.info(f"exec_sql: sql='{sql}' args={args}")
    return conn.execute(sql, args)


class SQLiteTypes(Enum):
    _type2member_map_: ClassVar[Dict[str, "SQLiteTypes"]] 

    def __init__(self, known_types, target_type):
        assert isinstance(known_types, tuple), f"known_types must be tuple, not {type(known_types)}"
        cls = self.__class__
        if not hasattr(cls, "_type2member_map_"):
            cls._type2member_map_ = {}
        for k in known_types:
            assert k in KNOWN_TYPES, f"Unknown type {k}"
            cls._value2member_map_[k] = self
        self.known_types = known_types
        self.target_type = target_type

    INTEGER = ( ("int" ,'bool'), int)
    REAL = ( ("float",),  float)
    TEXT = ( ("str", "date", "datetime", "path", "interval"), str)
    BLOB = ( ("blob",), bytes)

    @classmethod
    def from_known_type(cls, known_type:Union[KnownType, str])->"SQLiteTypes":
        return cast(SQLiteTypes, cls._value2member_map_[known_type if isinstance(known_type, str) else known_type.name])


class SQLiteTable:
    """
    Represents a SQLite table.

    Attributes:
        table (Table): The table object associated with this SQLiteTable.
        name (str): The name of the table.
        pkeys (str): The comma-separated string of primary key column names.

    Methods:
        create_table_sql(): Returns the SQL statement for creating the table.
        has_table(conn): Checks if the table exists in the database.
        ensure_table(conn): Ensures that the table exists in the database.
        insert(conn, *values): Inserts values into the table.

    """

    def __init__(self, table:Table) -> None:
        self.table = table
        self.name = table.name
        self.pkeys = ", ".join( k.name for k in table.fields.values() if k.is_key)
        all_cols = ", ".join(k.name for k in table.fields.values())
        placeholders = ", ".join("?" for _ in range(len(table.fields)))
        self._insert_sql = f"insert into {self.name} ({all_cols}) values ({placeholders})"

    def create_table_sql(self):
        def field_ddl(f:ArgField)->str:
            s = f"{f.name} {SQLiteTypes.from_known_type(f.type).name}"
            if f.default is not None and f.default.default is not None:
                s += f" DEFAULT {f.default.default!r}"
            return s
        all_defs = ", ".join( map(field_ddl, self.table.fields.values()) )
        if_pkeys = f", primary key ({self.pkeys})" if self.pkeys else ""
        return f"create table {self.table.name} ({all_defs}{if_pkeys})"

    def has_table(self, conn) -> bool:
        """
        Checks if the table exists in the database.

        Args:
            conn: The SQLite connection object.

        Returns:
            bool: True if the table exists, False otherwise.

        """
        return bool(
            len(
                conn.execute(
                    f"select 1 from sqlite_master where name = '{self.name}'"
                ).fetchall()
            )
        )

    def ensure_table(self, conn):
        """
        Ensures that the table exists in the database.

        Args:
            conn: The SQLite connection object.

        """
        if not self.has_table(conn):
            exec_sql(conn, self.create_table_sql())

    def insert(self, conn, *values):
        """
        Inserts values into the table.

        Args:
            conn: The SQLite connection object.
            *values: The values to be inserted into the table.

        """
        cur = exec_sql(conn, self._insert_sql, *values)
        assert cur.rowcount == 1


class AsOfState(DnState):
    def __init__(self, config:Dict[str,Any] ):
        config = config.copy()
        self.dbm_key = config.pop('dbm_key')
        self.key_config_list = config.pop('keys', None)
        assert config == {}, f"Unexpected entries {config}"
        self.keys: List[ArgField] = None
        self.node: DataNode = None
        self.table: SQLiteTable = None

    def init_with_node(self, node:DataNode):
        self.node = node
        # resolve keys
        if self.keys is None:
            if self.key_config_list is not None:
                self.keys = [ArgField.from_dict(d) for d in self.key_config_list]
            else:
                self.keys = list(map(copy, node.compute.args))
        # build table
        fields = []
        for k in self.keys:
            k.is_key = True
            fields.append(k)
        fields.append(ArgField("date", "date", is_key=True))
        fields.append(ArgField("text", "str"))
        self.table = SQLiteTable(Table(self.node.path.table(), fields))

    def _stmt_keys(self, after=", ", delim="") -> str:
        return delim.join(f"{k.name}{after}" for k in self.keys)

    def read(self, as_of_date: date, interval:Interval, *key_values) -> Tuple[date, str]:
        with self.get_conn() as conn:
            if self.table.has_table(conn):
                cur = exec_sql(
                    conn,
                    f"select date, text from {self.table.name} " 
                    f"where {self._stmt_keys(after='=? AND ')} date<=? " 
                    f"order by date desc",
                    *key_values, 
                    str(as_of_date)
                )
                rec = cur.fetchone()
                cur.close()
                if rec:
                    d = date.fromisoformat(rec[0])
                    if interval.match(d, as_of_date):
                        return (d, rec[1])
            return (None, None)

    def get_conn(self):
        return ctx.config.get().dbm[self.dbm_key].connection()

    def write(self, text:str, as_of_date:date, *key_values) -> None:
        with self.get_conn() as conn:
            self.table.ensure_table(conn)
            try:
                self.table.insert(conn, *key_values, str(as_of_date), text)
            except sqlite3.IntegrityError:
                cur = exec_sql(
                    conn,
                    f"update {self.table} set text=? "
                    f"where {self._stmt_keys('=? AND ')} date=?",
                    text, 
                    *key_values, 
                    str(as_of_date)
                )
                assert cur.rowcount == 1

    def get_distinct_keys(self, as_of_date:date, interval:Interval) -> pd.DataFrame:
        assert self.keys, f"No keys defined for {self.node.path}"
        with self.get_conn() as conn:
            if not self.table.has_table(conn):
                raise ValueError(f"No data for {self.node.path}")
            cur = conn.execute(
                f"select distinct {self._stmt_keys(after='', delim=', ')} from {self.table.name} "
                f"where date >= ? and date<=?",
                (str(as_of_date-interval.timedelta()), str(as_of_date)),
            )
        return pd.DataFrame(cur.fetchall(), columns=[k.name for k in self.keys]) 

class OnExpireStrategy(Enum):
    purge = False
    keep = True

    @classmethod
    def from_string(cls, s: str) -> "OnExpireStrategy":
        return cls[s.lower()]

    def is_for_keeps(self):
        return self.value

def cron_clean_cache(path:DataPath, task:str, trigger_time:datetime):
    dn = ctx.config.get().dn(path)
    if dn.cache is not None:
        log.info(f"Cleaning cache path={path}, task={task}, trigger_time={trigger_time}")
        #TODO implement cache cleaning


class TimedCache(DnCache):
    def __init__(self, config:Dict[str,Any]):
        config = dict(config)
        self.expire = Interval.from_string(config.pop('expire'))
        self.on_expire = OnExpireStrategy.from_string(config.pop("on_expire"))
        assert config == {}, f"Unexpected entries {config}"

    def get(self, dne:DnEvent) -> Any:
        cache_params = dne.get_cache_params(self.expire)
        text = None
        recompute = cache_params.force
        if not recompute:
            up_to_date, text =  self.node.state.read(dne.as_of_date, cache_params.get_interval(), *dne.typed_values)
            recompute = not(up_to_date)
        if recompute:
            self.compute_and_update_cache(dne)
            up_to_date, text = self.node.state.read(dne.as_of_date, cache_params.get_interval(), *dne.typed_values)
            assert up_to_date
        return from_json(json_loads(text))

    def compute_and_update_cache(self, dne:DnEvent) -> Any:
        data = self.node.compute.calculate(dne)
        self.node.state.write(
            json_dumps(to_json(data)), dne.as_of_date, *dne.typed_values
        )
        return data

    def get_distinct_keys(self, as_of_date:date, interval:Interval = None) -> pd.DataFrame:
        if interval is None:
            interval = self.expire
        return self.node.state.get_distinct_keys(as_of_date, interval)
