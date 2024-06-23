from functools import total_ordering
from typing import List, Optional, Tuple, Union


@total_ordering
class DataPath:
    """
    >>> abc = DataPath.ensure_path("a/b/c")
    >>> abc
    DataPath(('a', 'b', 'c'))
    >>> str(abc)
    'a/b/c'
    >>> abc.name()
    'c'
    >>> xyz = path("x/y/z")
    >>> zyx = DataPath.ensure_path("z/y/x")
    >>> abc < xyz
    True
    >>> abc > zyx
    False
    >>> sorted([zyx, abc, abc.parent(), xyz])
    [DataPath(('a', 'b')), DataPath(('a', 'b', 'c')), DataPath(('x', 'y', 'z')), DataPath(('z', 'y', 'x'))]
    >>> x = {abc: 1,  zyx: 3, xyz: 2,}
    >>> x[xyz]
    2
    >>> abc.parent()
    DataPath(('a', 'b'))
    >>> pp=abc.parents()
    >>> pp
    [DataPath(()), DataPath(('a',)), DataPath(('a', 'b'))]
    >>> pp[0].is_root()
    True
    >>> abc.table()
    'a$b$c'
    >>> DataPath.ensure_path(abc) == abc
    True
    >>> DataPath.ensure_path(abc.parts) == abc
    True
    >>> abc / "d"
    DataPath(('a', 'b', 'c', 'd'))
    >>> abc / abc
    DataPath(('a', 'b', 'c', 'a', 'b', 'c'))
    >>> DataPath.ensure_path("").is_root()
    True
    """
    parts: Tuple[str, ...]

    def __init__(self, parts: Tuple[str, ...]) -> None:
        self.parts = parts

    @staticmethod
    def ensure_path(path: Union[str, "DataPath", Tuple[str, ...]]) -> "DataPath":
        if isinstance(path, DataPath):
            return path
        elif isinstance(path, tuple):
            return DataPath(path)
        elif isinstance(path, str):
            assert "$" not in path, f"Invalid path {path}"
            assert " " not in path, f"Invalid path {path}"
            parts = path.strip("/").split("/")
            if len(parts) == 1 and parts[0] == "":
                parts = []
            for p in parts:
                assert p, f"Invalid path {path}"
            return DataPath(tuple(parts))
        else:
            raise AssertionError("Unsupported input", path)

    def is_root(self) -> bool:
        return len(self.parts) == 0

    def name(self) -> str:
        return self.parts[-1] if self.parts else ""

    def parent(self) -> Optional["DataPath"]:
        if len(self.parts) > 0:
            return DataPath(self.parts[:-1])
        return None

    def parents(self) -> List["DataPath"]:
        parents: List[DataPath] = []
        p = self
        while True:
            p = p.parent()
            if p is None:
                break
            parents.insert(0, p)
        return parents

    def table(self):
        return "$".join(self.parts)

    def __truediv__(self: "DataPath", other: Union["DataPath", str]) -> "DataPath":
        return DataPath(self.parts + DataPath.ensure_path(other).parts)

    def __eq__(self, v: object) -> bool:
        return isinstance(v, DataPath) and self.parts == v.parts

    def __lt__(self, v: object) -> bool:
        return isinstance(v, DataPath) and self.parts < v.parts

    def __str__(self) -> str:
        return "/".join(self.parts)

    def __repr__(self) -> str:
        return f"DataPath({self.parts!r})"

    def __hash__(self) -> int:
        return hash(self.parts)


def path(path: Union[str, "DataPath", Tuple[str, ...]]) -> "DataPath":
    return DataPath.ensure_path(path)

