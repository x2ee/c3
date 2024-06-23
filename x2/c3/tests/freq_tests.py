from datetime import date, timedelta

import pytest
from x2.c3.periodic import IntervalUnit, Interval
from pathlib import Path

def test_period():
    assert IntervalUnit.D == IntervalUnit.from_string('d')
    assert IntervalUnit.W == IntervalUnit.from_string('w')
    assert IntervalUnit.M == IntervalUnit.from_string('M')

    dates = [date(1999,1,1), date(1999,4,2), date(1999,7,2), date(1999,10,1), date(1999,12,31)]
    for d1,d2 in zip(dates[:-1], dates[1:]):
        assert d1 + IntervalUnit.Q.timedelta() == d2

def test_freq():
    dates = [date(1999,4,2), date(1999,7,2), date(1999,10,1), date(2000,1,1)]
    for n in range(1,5):
        d = date(1999,1,1) + Interval.from_string(f'{n}Q').timedelta()
        assert d == dates[n-1]
    try:
        Interval.from_string('1z')
        assert False
    except ValueError as e:
        assert str(e) == "('Invalid frequency string', '1z')"


def test_find_file():
    d = Path('build') / 'test_dir'
    d.mkdir(exist_ok=True, parents=True)
    file_dates = [date(1999,1,1), date(1999,1,8), date(1999,1,16), date(1999,1,23) ]
    files = [d / f"{p}{fd:%Y%m%d}.csv" for p in ["", "a"]  for fd in file_dates]
    for t in files:
        t.touch()
    f = Interval(1, IntervalUnit.W)
    test_dates = [date(1998,12,31) + timedelta(days=days) for days in  range(35)]
    matches = tuple(f.find_file(d, as_of) for as_of in test_dates)
    assert matches[0] is None
    assert tuple(matches[1:8]) == (files[0],)*7
    assert tuple(matches[8:15]) == (files[1],)*7
    assert matches[15] is None
    assert tuple(matches[16:23]) == (files[2],)*7
    assert tuple(matches[23:30]) == (files[3],)*7
    assert len(matches[30:]) == 5
    assert sum(map(lambda x: x is None,matches[30:])) == 5
