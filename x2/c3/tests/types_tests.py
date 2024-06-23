import io
import pandas as pd
import numpy as np
import x2.c3.types as t 


a = pd.DataFrame({"a": ["s1", "", "s3"], "b": [4, 5, 6]})
b = pd.DataFrame({"x": ["1", "", "3"], "y": [4, 5.1, 6]})

def test_json_conversions():
    assert (
        str(t.df_to_json(a)) ==
        "{'type_ref$': 'pandas.core.frame:DataFrame', 'series': {'a': {'dtype': 'object', 'data': ['s1', '', 's3']}, 'b': {'dtype': 'int64', 'data': [4, 5, 6]}}}")

    assert t.json_to_df(t.df_to_json(a)).equals(a)

    obj_ref = {
        "a": a,
        "c": {
            "d": 3,
            "e": 4.
        },
        "aa": [a, 1, None, {"b": b}],
    }
    assert t.to_json(t.from_json(t.to_json(obj_ref))) == {
        "a": {
            "type_ref$": "pandas.core.frame:DataFrame",
            "series": {
                "a": {"dtype": "object", "data": ["s1", "", "s3"]},
                "b": {"dtype": "int64", "data": [4, 5, 6]},
            },
        },
        "c": {"d": 3, "e": 4.0},
        "aa": [
            {
                "type_ref$": "pandas.core.frame:DataFrame",
                "series": {
                    "a": {"dtype": "object", "data": ["s1", "", "s3"]},
                    "b": {"dtype": "int64", "data": [4, 5, 6]},
                },
            },
            1, None,
            {
                "b": {
                    "type_ref$": "pandas.core.frame:DataFrame",
                    "series": {
                        "x": {"dtype": "object", "data": ["1", "", "3"]},
                        "y": {"dtype": "float64", "data": [4.0, 5.1, 6.0]},
                    },
                }
            },
        ],
    }

def test_numpy_encoder():

    assert t.json_dumps(np.bool_(True) ) == "true"  
    assert t.json_dumps(np.float64(5)) == "5.0"
    aa = np.array([np.float64(5), np.float64(4)])
    assert t.json_dumps(aa) == "[5.0, 4.0]"
    assert t.json_dumps(np.ndarray((2,), buffer=aa)) == "[5.0, 4.0]"
    assert t.json_dumps( np.void(b'') ) == "null"

    class X:
        pass

    try:
        t.json_dumps( X() ) 
        assert False
    except TypeError as te:
        assert str(te) == "Object of type X is not JSON serializable"


def test_known_types():
    df_type = t.KnownType.ensure("dataframe")
    df_type = t.KnownType.ensure(df_type)
    assert df_type.name == "dataframe"
    assert df_type == t.KnownType.ensure(pd.DataFrame)

def test_type_conv_matrix():
    ss = {
        "a": '{"type_ref$": "pandas.core.frame:DataFrame", "series": {"a": {"dtype": "object", "data": ["s1", "", "s3"]}, "b": {"dtype": "int64", "data": [4, 5, 6]}}}',
        "b": '{"type_ref$": "pandas.core.frame:DataFrame", "series": {"x": {"dtype": "object", "data": ["1", "", "3"]}, "y": {"dtype": "float64", "data": [4.0, 5.1, 6.0]}}}',
    }
    def round_trip(df):
        return t.convert_to_type(
            t.convert_to_type(t.convert_to_type(df, str), pd.DataFrame), str
        )
    assert t.convert_to_type(a, str) == ss["a"]
    assert t.convert_to_type(b, str) == ss['b']
    assert round_trip(a) == ss["a"]
    assert round_trip(b) == ss["b"]

    # test convert()
    tcm = t.TypeConversionMatrix(*t.TYPES_CONVERSIONS.mappings())
    assert tcm.convert(None, str) is None
    assert tcm.convert("s", str) == "s"
    assert tcm.convert("5", int) == 5
    x = {"a": 1, "b": 2}
    try:
        tcm.convert(x, int) 
        assert False
    except TypeError as te:
        assert str(te).startswith("int() argument ")

    tcm.add(dict, int, None)
    try:
        tcm.convert(x, int)
        assert False
    except ValueError as te:
        assert (
            str(te)
            == "Cannot convert from <class 'dict'> to <class 'int'>, `None` conversion defined"
        )
