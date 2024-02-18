"""
Interfaces:
* Tabular - class implements a convenient iterator across table data (query result).
    Access to column in each row is possible via correspond property name
* TabularStatic - static methods holder
* query_iterator - helper for performing query and iterating over its result
* batched_select_iterator - the same as `query_iterator` but performs batched query

USAGE EXAMPLES:

    cols = ['idx', 'square', 'inc', 'string']
    rows = [(i, i * i, i + 1, str(i)) for i in range(10)]
    filtered = [row.raw for row in Tabular(rows, cols) if row.square % 2 == 0]
    for row in Tabular(filtered, cols):
        print(row.idx, row.square)

    ============================================================

    query = "SELECT id as uid, person_name, email FROM userdata.user ORDER BY id"
    for user in query_iterator(CONN_ID, query):
        print(user.uid, user.person_name, user.email)

"""
from copy import deepcopy
from operator import itemgetter
from typing import List, Tuple, TypeVar, Type, Dict, Any, Generator, Union, Iterable, Set, Optional
from decimal import Decimal
from backend_tools.sqlwerks import execute_query_env as _execute_query_env

# generic type for model class
T = TypeVar('T')

# dictify key types
Key = Union[str, int]
CompoundKey = Union[List[Key], Tuple[Key, ...]]
CompoundStr = Union[str, List[str], Tuple[str, ...]]

# dictify result
Dictified = Dict[Any, Union[T, List[T]]]

__all__ = ('Tabular', 'TabularStatic', 'query_iterator', 'batched_select_iterator')


class Tabular:

    def __new__(cls, rows, cols, key=None, reverse=None):
        properties = ['rows', 'cols', 'raw']
        not_allowed = [entry for entry in dir(cls) + properties if entry in cols]
        if not_allowed:
            raise ValueError('Column list contains not allowed names: {}'.format(not_allowed))

        attrs = {col: cls.column_idx(idx) for idx, col in enumerate(cols)}
        dynamic_tabular = type('DynamicTabular', (cls,), attrs)
        return object.__new__(dynamic_tabular)

    def __init__(
            self, rows: List[Tuple], cols: Union[List[str], Tuple[str, ...]],
            key: callable = None, reverse: bool = False):
        if len(cols) > len(set(cols)):
            raise ValueError('Column list must contain unique values')
        if len(rows) > 0 and len(rows[0]) != len(cols):
            raise ValueError('Row length ({}) != column list ({})'.format(len(rows[0]), len(cols)))

        # columns
        self.cols = tuple(cols)
        # query result rows
        self.rows = rows
        # contains current raw row
        self.raw = None

        if key is not None:
            row_weights = [key(row) for row in self]
            self.rows = [rows[idx] for idx in sorted(list(range(len(rows))), key=lambda idx: row_weights[idx])]
            if reverse:
                self.rows = self.rows[::-1]

    def __str__(self):
        result = ['[{}] '.format(','.join(self.cols))]
        result.extend('({}) '.format(','.join(str(val) for val in row)) for row in self.rows[:10])
        result.extend(['...', 'Total {} rows'.format(len(self.rows))]) if len(self.rows) > 10 else None
        return '\n'.join(result)

    __repr__ = __str__

    def __iter__(self):
        for row in self.rows:
            self.raw = row
            yield self

    def __len__(self):
        return len(self.rows)

    def __bool__(self):
        return len(self.rows) > 0

    def __getitem__(self, item):
        if isinstance(item, slice):
            return Tabular(self.rows[item], self.cols)
        else:
            new_obj = Tabular([], self.cols)
            new_obj.raw = self.rows[item]
            return new_obj

    def __eq__(self, other):
        return isinstance(other, Tabular) and self.cols == other.cols and self.rows == other.rows

    def __ne__(self, other):
        return not self.__eq__(other)

    def __add__(self, other):
        if not isinstance(other, Tabular):
            raise TypeError('unsupported operand type(s) for +: "Tabular" and "{}"'.format(type(other)))
        elif other.cols != self.cols:
            raise ValueError('Cannot add Tabulars with different columns')
        return Tabular(self.rows + other.rows, self.cols)

    def __copy__(self):
        return Tabular(self.rows, self.cols)

    def __deepcopy__(self, memodict):
        result = Tabular(self.rows, self.cols)
        memodict[id(self)] = result
        result.rows = deepcopy(self.rows)
        result.cols = deepcopy(self.cols)
        return result

    @classmethod
    def column_idx(cls, idx):
        def core(obj):
            return obj.raw[idx]

        return property(core)

    def single_row(self) -> 'Tabular':
        """ Returns `single-row object` - gives possibility to direct access to row fields without iteration """
        self.raw = self.rows[0]
        return self

    def extend(self, other):
        """ Appends rows from another Tabular to the current one. Another Tabular must have the same columns """
        if not isinstance(other, Tabular):
            raise TypeError('"Tabular" cannot be extended with "{}"'.format(type(other)))
        elif other.cols != self.cols:
            raise ValueError('Cannot concatenate Tabulars with different columns')
        self.rows.extend(other.rows)

    def extract_column(self, column: str) -> List:
        """ Returns values list by specified column name """
        col_idx = self.cols_idxs([column])[0]
        return [row[col_idx] for row in self.rows]

    def cols_idxs(self, cols: Iterable[str] = None) -> List[int]:
        """
        Returns list of column indexes corresponds to its names

        :param cols: wanted column names list
        """
        if not cols:
            return list(range(len(self.cols)))

        try:
            return [self.cols.index(col) for col in cols]
        except ValueError:
            raise ValueError('"{}" is not valid column list'.format(cols))

    def to_dicts(self, cols: List[str] = None) -> Generator[Dict[str, Any], None, None]:
        """
        Returns generator which converts every row into a dict

        :param cols: column names needed in a dict
        """

        if not cols:
            cols = self.cols
        idx_getter = itemgetter(*self.cols_idxs(cols)) if len(cols) > 1 else lambda r: (r[0], )
        return (dict(zip(cols, idx_getter(row))) for row in self.rows)

    def as_dicts(
            self, cols: List[str] = None, excluded_cols: List[str] = None, names_map: Dict[str, str] = None,
            format_map: Dict[str, callable] = None, updater: callable = None) -> Generator[Dict[str, Any], None, None]:
        """
        Returns generator which converts every row into a dict (`to_dicts` extended version)
        Excludes keys which values is None

        :param cols: column names needed in a dict (except excluded_cols)
        :param excluded_cols: column names for exclude from dict (even if they are specified in `cols`)
        :param names_map: mapping for columns renaming
        :param format_map: mapping for values transform
        :param updater: function for dict post-processing
            signature: callback(obj: Dict) -> Object
        """

        if not cols:
            cols = self.cols
        if excluded_cols:
            cols = [col for col in cols if col not in excluded_cols]
        idx_getter = itemgetter(*self.cols_idxs(cols))
        if names_map:
            cols = [names_map.get(col, col) for col in cols]
            if format_map:
                format_map = {names_map.get(col, col): v for col, v in format_map.items()}

        result = (
            dict(kv for kv in zip(cols, _row_serializable(idx_getter(row))) if kv[1] is not None) for row in self.rows)
        if format_map:
            result = ({k: format_map[k](v) if k in format_map else v for k, v in row.items()} for row in result)
        if updater:
            result = (updater(row) for row in result)
        return result

    def to_models(
            self, model: Type[T], cols: List[str] = None, rename_map: Dict[str, str] = None
    ) -> Generator[T, None, None]:
        """
        Returns generator which converts every row into a model

        :param model: wanted model, Model class descedant
        :param cols: columns list which must be in model (all columns by default)
        :param rename_map: mapping for renaming tabular columns into model properties, optional
        """
        if rename_map is None:
            rename_map = {}
        if not cols:
            cols = self.cols
        idx_getter = itemgetter(*self.cols_idxs(cols)) if len(cols) > 1 else lambda r: (r[0], )
        return (
            model(**dict(zip(tuple(rename_map.get(col, col) for col in cols), idx_getter(row)))) for row in self.rows)

    def groupby(self, key: Union[Key, CompoundKey]) -> Dictified:
        """ Groups rows by given key. Each group is returned as Tabular """
        return TabularStatic.dictify_tabular(key, self, overwrite=False, tabular_rows=True)

    def dictify_tabular(
            self, key: Union[Key, CompoundKey], overwrite=True, tabular_rows=False, exclude_key=False,
            value_map=lambda v: v) -> Dictified:
        """ Helper for `dictify_tabular` """
        return TabularStatic.dictify_tabular(key, self, overwrite, tabular_rows, exclude_key, value_map)

    def merge(self, other: 'Tabular', key: CompoundStr) -> 'Tabular':
        """ Merges columns of two tabulars by given key """
        return TabularStatic.merge(self, other, key)

    def left_join(self, other: 'Tabular', key: CompoundStr, default: Dict[str, Any] = None) -> 'Tabular':
        """
        Left join corresponding row from other to each row from self.
        Fills non existent rows with values from `default` if specified
        """
        return TabularStatic.left_join(self, other, key, default=default)

    def inner_join(self, other: 'Tabular', key: CompoundStr) -> 'Tabular':
        """
        Joins corresponding row from other to each row from self.
        Skips rows without correspondence
        """
        return TabularStatic.inner_join(self, other, key)

    def unnest(
            self, column: CompoundStr,
            formatter: Union[Optional[callable], Dict[str, Optional[callable]]] = None) -> 'Tabular':
        """
        Unnests items of specified columns
        Applies `formatter` to each element, if specified
        """
        return TabularStatic.unnest(self, column, formatter=formatter)

    def col_map(self, column: CompoundStr, func: callable) -> 'Tabular':
        """ Applies func to each element of specified column """
        return TabularStatic.col_map(self, column, func)

    def filter(self, key: callable, raw: bool = False) -> 'Tabular':
        """ Returns new Tabular with rows for which `key` function returns True """
        return TabularStatic.filter(self, key, raw=raw)

    def sort(self, key: callable, raw: bool = False, reverse: bool = False) -> 'Tabular':
        """ Returns new Tabular with sorted rows by specified `key` function """
        return TabularStatic.sorted(self, key, raw=raw, reverse=reverse)

    def only(self, columns: Union[List[str], Tuple[str]]):
        """ Returns new Tabular which contains specified columns only """
        return TabularStatic.filter_columns(self, columns)

    def col_rename(self, col_from, col_to) -> 'Tabular':
        """ Returns new Tabular with renamed `col_from` to `col_to` """
        return Tabular(self.rows, [col if col != col_from else col_to for col in self.cols])
    


class TabularStatic:
    @classmethod
    def dictify(
            cls, key: Union[Key, CompoundKey], iterable: Iterable[T], overwrite=True, exclude_key=False) -> Dictified:
        """
        Converts iterable into a dict. As a key uses values of columns specified in `key`.
        `iterable` must support `__getitem__` method (list, tuple, dict, Model, etc).

        :param key: Column(s) index/name, which values wanted as a key
        :param iterable: source iterable
        :param overwrite: flag for overwrite strategy behaviour
            If True, then every key will have one dict only.
                In case of multiple values for the same key old value will be replaced by new one.
            If False, then every key will have a list of dicts
        :param exclude_key: flag for excluding `key` column(s) from resulting dict
        :return: dictified iterable
        """

        get_key = cls._get_key
        process_row = cls._process_row
        if overwrite:
            return {get_key(row, key): process_row(row, key, exclude_key) for row in iterable}

        result = {}
        for row in iterable:
            key_value = get_key(row, key)
            result.setdefault(key_value, []).append(process_row(row, key, exclude_key))
        return result

    @classmethod
    def dictify_tabular(
        cls, key: Union[Key, CompoundKey], iterable: Tabular,
        overwrite=True, tabular_rows=False, exclude_key=False, value_map=lambda v: v
    ) -> Dictified:
        """
        Extended version of `dictify`. Supports Tabular as an iterable source.
        If `tabular_rows` == True, then values converts to Tabulars
        """
        newkey = key
        if isinstance(key, str):
            newkey = iterable.cols.index(key)
        elif isinstance(key, (list, tuple)):
            newkey = [iterable.cols.index(k) if isinstance(k, str) else k for k in key]

        dictified = cls.dictify(newkey, iterable.rows, overwrite=overwrite, exclude_key=exclude_key)
        if not tabular_rows:
            return {k: value_map(v) for k, v in dictified.items()}

        new_cols = [col for col in iterable.cols if col != key] if exclude_key else iterable.cols

        return {
            k: value_map(Tabular([group], new_cols).single_row()) if overwrite else value_map(Tabular(group, new_cols))
            for k, group in dictified.items()}

    @classmethod
    def merge(cls, source: Tabular, other: Tabular, column: CompoundStr) -> Tabular:
        """
        Returns new Tabular with merged columns from `source` and `other` by given key.
        `other` must have all the keys from `source`.
        `other` must have unique values for key.
        :param source:
        :param other:
        :param column: column(s) which wanted as a key, must be in both Tabulars
        """
        return cls._merge_engine(source, other, column)

    @classmethod
    def left_join(cls, source: Tabular, other: Tabular, column: CompoundStr, default: Dict[str, Any] = None) -> Tabular:
        """
        Returns new Tabular with merged columns from `source` and `other` by given key (left join strategy)
        `other` can have not all the keys from `source` (see `default` for details).
        `other` can have duplicate values for key (corresponds rows from `source` will be duplicated)
        :param source:
        :param other:
        :param column: column(s) which wanted as a key, must be in both Tabulars
        :param default: values which will be added to result in case if key from `source` is not present in `other`.
            if None - to result will be added `None` for every column from `other`
        """
        return cls._merge_engine(source, other, column, default=default, allow_default=True)

    @classmethod
    def inner_join(cls, source: Tabular, other: Tabular, column: CompoundStr) -> Tabular:
        """
        Returns new Tabular with merged columns from `source` and `other` by given key (inner join strategy)
        `other` can have not all the keys from `source`
            rows from `source` with no corresponds in `other` will be omited
        `other` can have duplicate values for key (corresponds rows from `source` will be duplicated)
        :param source:
        :param other:
        :param column: column(s) which wanted as a key, must be in both Tabulars
        """
        return cls._merge_engine(source, other, column, skip_not_found=True)

    @staticmethod
    def unnest(
            data: Tabular, column: CompoundStr,
            formatter: Union[Optional[callable], Dict[str, Optional[callable]]] = None) -> Tabular:
        """
        Performs transformations on Tabular similar to postgresql `unnest` command:
            iterable value from `column` expands to a set of rows,
            for each new row the rest columns values copies from the source row
        Returns new Tabular

        :param data: source Tabular
        :param column: column (or list of columns) which contains iterable values
        :param formatter: function (or dict: {<column_name>: <formatter_func>})
            applied to values of corresponding column BEFORE expand, optional
        """
        if not isinstance(data, Tabular):
            raise TypeError('`source` must be instance of `Tabular`')
        if not column:
            raise ValueError('`column` must not be empty')
        if not len(data):
            # nothing to do here
            return Tabular([], data.cols)

        cols = column if isinstance(column, (list, tuple)) else [column]
        cols_idx = data.cols_idxs(cols)

        # column order changes: unnested columns moved to end, tabulars add new columns
        result_cols = [col for col in data.cols if col not in cols]
        first_row = data.rows[0]
        for col_idx in cols_idx:
            if isinstance(first_row[col_idx], Tabular):
                result_cols.extend(first_row[col_idx].cols)
            else:
                result_cols.append(data.cols[col_idx])

        width = len(result_cols)
        if len(set(result_cols)) != width:
            raise ValueError('Column list must contain unique values')

        formatters = formatter if isinstance(formatter, dict) else {col: formatter for col in cols}
        formatters = {col_idx: formatters.get(col) or (lambda x: x) for col, col_idx in zip(cols, cols_idx)}

        result_rows = []
        left_cols_idx = [idx for idx, col in enumerate(data.cols) if col not in cols]
        for row in data.rows:
            # make left unchanged part of new row
            new_row_left = tuple(row[col_idx] for col_idx in left_cols_idx)
            # collect and format items to be unnested
            items = [
                item if isinstance(item, (list, Tabular)) else [item]
                for item in (formatters[col_idx](row[col_idx]) for col_idx in cols_idx)]
            # check length
            len_i = len(items[0])
            if not all(len(item) == len_i for item in items[1:]):
                raise ValueError('Elements must be of identical length when unnesting multiple columns')
            if len_i == 0:
                # nothing to unnest - fill with emptiness
                items = [[None]] * (width - len(new_row_left))
            # make new rows
            for items_row in zip(*items):
                new_row_right = tuple()
                for new_right_item in items_row:
                    if isinstance(new_right_item, Tabular):
                        new_row_right += new_right_item.raw
                    else:
                        new_row_right += (new_right_item,)
                new_row = new_row_left + new_row_right
                if len(new_row) != width:
                    raise ValueError('Unnested `Tabular` elements in each column must have equal number of columns')
                result_rows.append(new_row)
        return Tabular(result_rows, result_cols)

    @staticmethod
    def col_map(data: Tabular, column: CompoundStr, func: callable) -> Tabular:
        """
        Returns new Tabular, applies `func` to each value of `column`
        :param data: source Tabular object
        :param column: wanted column name to transformation
        :param func: transform function
        """
        cols = column if isinstance(column, (list, tuple)) else [column]
        cols_idx = data.cols_idxs(cols)

        def row_func(row: tuple) -> tuple:
            new_row = list(row)
            for col_idx in cols_idx:
                new_row[col_idx] = func(row[col_idx])
            return tuple(new_row)

        rows = [row_func(row) for row in data.rows]
        return Tabular(rows, data.cols)

    @staticmethod
    def filter_columns(data: Tabular, columns: Union[List[str], Tuple[str]]) -> Tabular:
        """
        Retunrs new Tabular which contains specified columns only (in appropriate order)
        :param data: source Tabular
        :param columns: wanted column names for the resulting Tabular, must be a subset of `data.cols`
        """
        idxs = data.cols_idxs(columns)
        rows = [tuple(row[idx] for idx in idxs) for row in data.rows]
        return Tabular(rows, columns)

    @staticmethod
    def filter(data: Tabular, key: callable, raw=False) -> Tabular:
        """
        Returns new Tabular with filtered rows via specified `key` function
        :param data: source Tabular
        :param key: filter function, applied to each row, must return True if row wanted in result
        :param raw: flag for processing raw rows
            True - `key` gets row as a tuple
            False - `key` gets Tabular row
        """
        rows = [row for row in data.rows if key(row)] if raw else [row.raw for row in data if key(row)]
        return Tabular(rows, data.cols)

    @staticmethod
    def sorted(data: Tabular, key: callable, raw=False, reverse=False) -> Tabular:
        """
        Returns new Tabular with sorted rows via specified `key` function
        :param data: source Tabular
        :param key: sort function, applied to each row
        :param raw: flag for processing raw rows
            True - `key` gets row as a tuple
            False - `key` gets Tabular row
        :param reverse: flag for reversing sort result
        """
        if raw:
            rows = sorted(data.rows, key=key)
            rows.reverse() if reverse else None
            return Tabular(rows, data.cols)
        else:
            return Tabular(data.rows, data.cols, key=key, reverse=reverse)

    @classmethod
    def _merge_engine(
            cls, source: Tabular, other: Tabular, column: CompoundStr,
            default: Dict[str, Any] = None, allow_default=False, skip_not_found=False) -> Tabular:
        if not isinstance(source, Tabular) or not isinstance(other, Tabular):
            raise TypeError('Both `source` and `other` must be instance of `Tabular`')
        cols = column if isinstance(column, (list, tuple)) else [column]
        cols_idx = source.cols_idxs(cols)

        # Замыкает cols_idx
        def key_getter(row):
            if isinstance(column, (list, tuple)):
                return tuple(row[i] for i in cols_idx)
            else:
                return row[cols_idx[0]]

        other_cols = list(other.cols)
        [other_cols.remove(col) for col in cols]

        if not allow_default and not skip_not_found:
            # merge
            other_dict = cls.dictify_tabular(column, other, exclude_key=True)
            if len(other_dict) < len(other):
                raise ValueError('other Tabular object must have unique values of key columns')
            if any(key_getter(row) not in other_dict for row in source.rows):
                raise ValueError('other Tabular object must have all keys present in source Tabular')
            new_rows = [row + other_dict[key_getter(row)] for row in source.rows]
        else:
            other_dict = cls.dictify_tabular(column, other, exclude_key=True, overwrite=False)
            if allow_default:
                # left_join
                default = default or {}
                default_values = [tuple(default.get(col) for col in other_cols)]
            else:
                # elif skip_not_found:
                # inner_join
                default_values = []
            new_rows = [
                row + other_row
                for row in source.rows
                for other_row in other_dict.get(key_getter(row), default_values)]

        new_cols = source.cols + tuple(other_cols)
        return Tabular(new_rows, new_cols)

    @staticmethod
    def _get_key(row, key):
        return tuple(row[k] for k in key) if isinstance(key, (list, tuple)) else row[key]

    @staticmethod
    def _process_row(
            row: Union[Tuple, List, Dict], key: Union[Key, CompoundKey], exclude_key: bool
    ) -> Union[Tuple, List, Dict]:
        """ Row transformation for `TabularStatic.dictify` """
        t = type(row)
        keys = set(key) if isinstance(key, (list, tuple)) else {key}
        if t in (list, dict):
            # TODO::OPTIMIZE find the way to omit the copy
            new_row = row.copy()
            [new_row.pop(k) for k in keys] if exclude_key else None
        elif t == tuple:
            new_row = tuple(entry for idx, entry in enumerate(row) if idx not in keys) if exclude_key else row
        else:
            raise ValueError('"exclude_key" usable only with list, tuple or dict')

        return new_row

    @staticmethod
    def serialize(data) -> Tuple[list, dict]:
        """ Converts Tabular to serializable objects """
        # if data = Tabular(...).single_row(), then need to save data.raw
        return [data.rows, data.cols, data.raw], {}

    @staticmethod
    def restore(*args):
        """ Deserializes Tabular """
        rows, cols, raw = args
        data = Tabular(rows, cols)
        data.raw = raw
        return data


def query_iterator(conn_id, query: str, params=None, values: Union[List, Tuple, Set, Dict] = None) -> Tabular:
    """
    Helper for iteration the query result (converts result into Tabular)
    Supports queries with `VALUES %s` (`values` arg)
    DO NOT supports following kwargs (its useless here):
        single_row, single_value, fail_result, commit, rollback
    """
    return Tabular(*_execute_query_env(conn_id, query, params=params, insert_many=tuple(values) if values else None))


def _row_serializable(row: Tuple):
    return tuple((float(val) if isinstance(val, Decimal) else val) for val in row)

