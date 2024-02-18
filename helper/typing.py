from typing import Union, List, Tuple, Set, Dict, Iterable

SizedIterable = Union[List, Tuple, Set, Dict]

RowsAndCols = Tuple[List, List]
SinglePGRow = Tuple[Tuple, List]

StrOrList = Union[str, Iterable[str]]
IntOrList = Union[int, Iterable[int]]