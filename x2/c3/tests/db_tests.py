from threading import Thread
from x2.c3.types import ArgField, Table
from x2.c3.db import SQLiteDbMap, SQLiteTable
import time, random, base64, pathlib
import pytest
from traceback import format_exc


class IThread(Thread):
    def __init__(self, i):
        super().__init__()
        self.i = i
        self.succeeded = False
        self.start()

    def __bool__(self):
        return self.succeeded

def test_sqlite_table():
    table = SQLiteTable(Table("test", [ArgField("id", "int"), ArgField("name", "str")]))
    assert table.create_table_sql() == 'create table test (id INTEGER, name TEXT)'
    table = SQLiteTable(Table("test", [ArgField("id", "int",is_key=True), ArgField("name", "str")]))
    assert (
        table.create_table_sql()
        == "create table test (id INTEGER, name TEXT, primary key (id))"
    )

@pytest.mark.slow
def test_pool():
    build_dir = pathlib.Path('build')
    build_dir.mkdir(exist_ok=True)
    for old in build_dir.glob('*.db'):
        old.unlink()
    random.seed(time.time())
    def random_db_name():
        return base64.b16encode(random.getrandbits(128).to_bytes(16, 'big')).decode()
    with SQLiteDbMap(build_dir, [random_db_name() for _ in range(2)]).add('mem',":memory:") as dbm:
        class PerDb(IThread):
            def run(self) -> None:
                self.ee = []
                self.succeeded = True
                try:
                    dbname = self.i
                    pool = dbm[dbname]
                    table = SQLiteTable(Table(
                        'test', 
                        [ArgField('id', 'int'), ArgField('name', 'str')]
                        ))
                    with pool.connection() as conn:
                        assert not table.has_table(conn)
                        table.ensure_table(conn)
                        assert table.has_table(conn)

                    class WriteThread(IThread):
                        def run(self):
                            try:
                                with pool.connection(max_wait=10) as conn:
                                    time.sleep(.1*self.i)
                                    conn.execute(f'insert into test values ({self.i}, "{self.i}")')
                                    if self.i == 10:
                                        raise ValueError('Test rollback')
                                    self.succeeded = True
                            except Exception as e:
                                if e.args[0] == 'Test rollback':
                                    self.succeeded = True
                                print(e)

                    class ReadThread(IThread):
                        def run(self):
                            for _ in range(100):
                                with pool.connection(max_wait=20) as conn:
                                    rows = conn.execute(f'select * from test where id = {self.i}').fetchall()
                                if self.i == 10:
                                    assert not rows
                                    self.succeeded = True
                                elif len(rows) == 1 and rows[0][0] == self.i: 
                                    self.succeeded = True
                                    break
                                time.sleep(.1)

                    ww = [WriteThread(i) for i in range(11)]
                    rr = [ReadThread(i) for i in range(11)]
                    for w in ww: w.join()
                    assert all(ww)
                    for r in rr: r.join()
                    assert all(rr)

                    class ExhaustedThread(IThread):
                        def run(self):
                            try:
                                with pool.connection(max_wait=1) as conn:
                                    time.sleep(2)
                            except ValueError as e:
                                self.succeeded = e.args[0] == 'Connection pool exhausted'

                    eet = [ExhaustedThread(i) for i in range(2)]
                    for e in eet: e.join()
                    assert any(eet)
                except:
                    self.ee.append(format_exc())
                    self.succeeded = False
        db_threads = [PerDb(n) for n in dbm.keys()]
        for t in db_threads: t.join()
        if not all(db_threads):
            for t in db_threads:
                if t.ee:
                    print(f"Thread {t.i} failed ->>>>")
                    for e in t.ee: 
                        print(e)
            assert False
