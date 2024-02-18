from io import StringIO
from time import time
from typing import Iterable, List, Tuple, Dict, Union, Callable, Any, Optional, IO

from backend_tools.configuration import cfg_value
from backend_tools.postgres_drv import execute_query, execute_copy, QueryCanceledError, PGStatementTimeout, mogrify
from backend_tools.logger import debug, error, warning
from backend_tools.misc import write_to_io, elapsed_str, to_csv_value, get_last_error_message
from backend_tools.builder.where import where
from collections import KeysView, ValuesView


TExecuteQueryEnvResult = Union[Tuple[Any, List[str]], Tuple[Any, None]]


def timed_statement(func):
    """ Decorator for handling PG query timeouts """
    def core(*args, **kwargs):
        _timeout = False
        msg = None
        try:
            res = func(*args, **kwargs)

        except QueryCanceledError:
            # psycopg2.extensions.QueryCanceledError: canceling statement due to statement timeout
            # OR
            # psycopg2.extensions.QueryCanceledError: canceling statement due to user request
            res = None
            _timeout = True
            msg = get_last_error_message()

        if _timeout:
            #  import from .notifier cause to circular import => we cannot send notification here
            #     therefore pushing up traceback and the query params
            #     and changing the exception to DatabaseStatementTimeout descedant
            raise PGStatementTimeout(message=msg, params={'args': args, 'kwargs': kwargs})

        return res

    return core


@timed_statement
def execute_query_env(
        conn_id: str, query: Optional[str], label: str = None, timed: bool = False, fail_result: Any = None, **kwargs
) -> TExecuteQueryEnvResult:
    """
    Extension for execute_query. Added support for: label, timed, fail_result
        Can set behaviour for exception - output to log or raise an exception
        Details for possible kwargs - see backend_tools.postgres_drv.execute_query()

    :param query: SQL query
    :param conn_id: connection id in connection pool
    :param label: label for debug output, query otherwise
    :param timed: flag for logging query execution time (`debug` log level)
    :param fail_result: this value will be returned if there was an error (error message will be looged).
        If None - exception will be raised (preserving `execute_query` behaviour)

    :return:
        (<data_from_db>, <columns>) - if success
        (fail_result, None) - if was exception
    """
    debug(label or query)
    start = time()
    result, error_text = execute_query(conn_id, query=query, fail_result=fail_result, **kwargs)
    if error_text and fail_result is not None:
        error(error_text)
        result = fail_result, None

    debug('done in {}'.format(elapsed_str(start))) if timed else None
    return result


def do_commit(conn_id: str, timed: bool = False, label: str = 'COMMIT'):
    """ Helper for apply transaction (commit changes). See `execute_query_env` for parameters details """
    return execute_query_env(conn_id, query=None, commit=True, timed=timed, label=label)


def do_rollback(conn_id: str, label: str = 'ROLLBACK'):
    """ Helper for rollback transaction. See `execute_query_env` for parameters details """
    return execute_query_env(conn_id, query=None, rollback=True, label=label)


def drop_table(conn_id: str, table: str):
    """
    Helper for table remove
    :param conn_id:
    :param table: table name in format: <schema_name>.<table_name>
    """
    query = "DROP TABLE IF EXISTS {}".format(table)
    execute_query_env(conn_id, query=query, fetch_result=False)


def truncate_tables(conn_id: str, tables: Iterable[str]):
    """
    Helper for truncate tables
    :param conn_id:
    :param tables: list of table names in format: <schema_name>.<table_name>
    """
    query_tmpl = "TRUNCATE TABLE {};"
    execute_query_env(conn_id, query=''.join([query_tmpl.format(table) for table in tables]), fetch_result=False)


def timed_commit(conn_id: str):
    """ Helper for commit with logging execution time """
    return do_commit(conn_id, timed=True)


def table_exists(conn_id: str, table: str, schema: str = 'public') -> bool:
    """ Checks if the specified table exists """
    schema_name, table_name = table.split('.') if '.' in table else (schema, table)
    query = """
        SELECT TRUE FROM pg_class c
        LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE c.relkind = 'r' AND n.nspname = %s AND c.relname = %s
        LIMIT 1;
    """
    rows, _ = execute_query_env(conn_id, query=query, params=[schema_name, table_name])
    return bool(rows)


def create_schema(conn_id: str, schema_name: str, read_only_user: str = None):
    """
    Creates new schema in DB; grants read-only access to a specified user

    :param conn_id:
    :param schema_name: new schema name
    :param read_only_user: user for read-only access. If not specified - reads value from config.
        If config parameter is empty - ignores read-only access grant
    """
    read_only_user = read_only_user or cfg_value('READ_ONLY_USER')
    query = "CREATE SCHEMA IF NOT EXISTS {schema};"
    if read_only_user:
        query += """
            GRANT USAGE ON SCHEMA {schema} TO {user};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT SELECT ON TABLES TO {user};
            ALTER DEFAULT PRIVILEGES IN SCHEMA {schema} GRANT USAGE, SELECT ON SEQUENCES TO {user};
        """
    execute_query_env(conn_id, query=query.format(schema=schema_name, user=read_only_user), fetch_result=False)


def part_select(conn_id: str, query: str, params=None, tables: Iterable[str] = None):
    """
    Executes the same query to a list of tables (preserving order).
        Next table is requestring only if previous query returns nothing

    :param conn_id: connection id
    :param query: query template in format: SELECT ... FROM {} WHERE ...
        `{}` - placeholder for table name
    :param params: query parameters
    :param tables: table names list
    :return: first occurred result which contains one or more rows
    """
    tables = tables or []
    for table in tables:
        data, _ = execute_query_env(conn_id, query=query.format(table), params=params)
        if data:
            return data

    return None


def part_select_ex(
        conn_id: str, query: str, params: Union[List, Dict] = None, tables: Iterable[str] = None,
        format_key: str = None, single_value: bool = False, single_row: bool = False
) -> Union[Tuple[Any, List[str]], Tuple[None, None]]:
    """
    Extended version of `part_select`. Added support for: format_key, single_row, single_value

    :param conn_id: connection id
    :param query: query template in format: SELECT ... FROM {} WHERE ...
        `{}` - placeholder for table name (or custom, see below)
    :param params: query parameters
    :param tables: table names list
    :param format_key: custom placeholder for table name.
        Specify this if you want to use custom placeholder instead of `{}`
    :param single_value: see `execute_qeury` for details
    :param single_row: see `execute_qeury` for details
    :return: first occurred result which contains one or more rows
    """
    tables = tables or []
    for table in tables:
        q = query.format(table) if not format_key else query.format(**{format_key: table})
        rows, cols = execute_query_env(
            conn_id, query=q, params=params, single_value=single_value, single_row=single_row)
        # execute_query_env returns [] for empty result even if single_row/value specified
        if isinstance(rows, list) and not rows:
            continue
        return rows, cols
    return None, None


def aggregate_select(conn_id: str, query: str, params=None, tables: Iterable[str] = None) -> List:
    """
    Performs the query against a table list (preserving order).
        Requests all tables, aggregates results

    :param conn_id: connection id
    :param query: query template in format: SELECT ... FROM {} WHERE ...
        `{}` - placeholder for table name
    :param params: query parameters
    :param tables: table names list
    :return: aggregated queries results
    """
    tables = tables or []
    result = []
    for table in tables:
        data, _ = execute_query_env(conn_id, query=query.format(table), params=params)
        result.extend(data) if data else None

    return result


def aggregate_filter_select(
        conn_id: str, columns: Union[str, Iterable[str]] = None, tables: Iterable[str] = None,
        search_column=None, search_list=None, extra_where=None,
        default_value=None, known_values=None, raise_not_found: bool = False, unique: bool = True, ordered: bool = True,
        dict_result: bool = False, dict_exclude_not_found: bool = False
):
    """
    Implements query like: SELECT ... WHERE ... IN (...) in multiple tables with results aggregation

    Required parameters:
    :param conn_id: connection id
    :param columns: wanted columns
        str - one column - one value in result
        list(str) - columns list - tuple of values in result (in appropriate order)
    :param tables: table name list, name format: <schema_name>.<table_name>
    :param search_column: column for search
    :param search_list: values list for search
        if `search_list` contains duplicate values,
        then the function performs redundant actions and displays a warning in the log
        (except cases (unique==False, ordered==False) or (dict_result=True))

    Optional parameters:
    :param extra_where: additional where clause
    :param default_value: value will be placed in the result for all not found keys, None by default
    :param known_values: list of known values, which will be filtered, in format:
        [[result_1, result_2, ...], search_column_id] or [[result_1, result_2, ...]] (consider search_column_id = 0),
        legend:
            result_* - result objects,
            search_column_id - `search_column` index in each result object
    :param raise_not_found: flag for raising ValueError if at least one of `search_list` values was not found
    :param unique:
        True - for each `search_list` value corresponds only one result.
            If there are several results - will be returned the last found result only.
            If `search_list` contains duplicate values - correspond results will be duplicated
        False - for each `search_list` value can corresponds several results.
            But if we got at least one result from some table, that search key will be excluded for other tables.
            Use `aggregate_select()` if you need to get all the possible results.
            `default_value` not used, `raise_not_found` ignores, len(search_list) != len(result)
    :param ordered:
        True - results will be placed in appropriate order to `search_list` values
            If `search_list` contains duplicate values - correspond results will be duplicated
        False - results in random order
        If `unique` == True consider `ordered` = True even if its value is False
    :param dict_result:
        True - result will be a dict (~optimized version)
            key - value from `search_list`
            value - unique==True: found value or `default_value`
                    unique==False: list of found values or []
            `ordered` flag is ignored
        False - by default
    :param dict_exclude_not_found: if `dict_result` == True, then result contains keys for found values only
    :return: search results
    """
    assert isinstance(search_list, list), 'Invalid search_list value'
    assert search_column, 'Invalid search_column value'
    assert columns, 'Invalid columns value'
    if not search_list:
        return []

    search_in_columns = False
    single_value = False
    tables = tables or []
    columns_list = columns[:]

    if isinstance(columns_list, str):
        single_value = True
        columns_list = [columns_list]
    if search_column in columns_list:
        search_in_columns = True
        search_column_id = columns_list.index(search_column)
    else:
        search_column_id = len(columns_list)
        columns_list.append(search_column)

    columns_list = ', '.join(columns_list)

    if extra_where:
        extra_where = ' AND {}'.format(extra_where if isinstance(extra_where, str) else ' AND '.join(extra_where))
    else:
        extra_where = ''

    default_obj = [None]
    duplicates = False
    if dict_result:
        search_list = set(search_list)
        search_list_filter = search_list
        not_found_keys = []
        result = {}
    else:
        search_list_filter = search_list.copy()
        if unique or ordered:
            result = [default_obj] * len(search_list)
            not_found_keys = []
        else:
            not_found_keys = [True] * len(search_list)
            result = []

    if dict_result:
        def set_result_unique(_key, val):
            result[_key] = val

        def set_result_not_ordered(_key, val):
            if _key not in result:
                result[_key] = [val]
            else:
                result[_key].append(val)

        set_result_ordered = set_result_not_ordered  # cuz ordered ignored

    else:
        search_list_tmp = {val: idx for idx, val in enumerate(search_list)}
        if len(search_list_tmp) < len(search_list) and (unique or ordered):
            warning('aggregate_filter_select: search_list contains duplicates')
            duplicates = True
            search_list_tmp = {val: [] for val in search_list_tmp}
            for idx, val in enumerate(search_list):
                search_list_tmp[val].append(idx)

            def set_result_unique(_key, val):
                for idx in search_list[_key]:
                    result[idx] = val

            def set_result_ordered(_key, val):
                for idx in search_list[_key]:
                    if result[idx] is default_obj:
                        result[idx] = [val]
                    else:
                        result[idx].append(val)
        else:
            def set_result_unique(_key, val):
                result[search_list[_key]] = val

            def set_result_ordered(_key, val):
                idx = search_list[_key]
                if result[idx] is default_obj:
                    result[idx] = [val]
                else:
                    result[idx].append(val)

        def set_result_not_ordered(_key, val):
            not_found_keys[search_list[_key]] = False
            result.append(val)

        search_list = search_list_tmp

    if unique:
        set_result = set_result_unique
    else:
        if ordered:
            set_result = set_result_ordered
        else:
            set_result = set_result_not_ordered

    if known_values:
        if len(known_values):
            known_search_column_id = known_values[1]
        else:
            known_search_column_id = 0
        for known_value in known_values[0]:
            if known_value[known_search_column_id] in search_list:
                if single_value:
                    set_result(known_value[known_search_column_id], known_value[1 - known_search_column_id])
                else:
                    if search_in_columns:
                        set_result(known_value[known_search_column_id], known_value)
                    else:
                        set_result(known_value.pop(known_search_column_id), known_value)
        if dict_result:
            search_list_filter -= result.keys()
        else:
            search_list_filter.clear()
            for key in search_list:
                if result[search_list[key]] is default_obj:
                    search_list_filter.append(key)
        if not search_list_filter:
            return result

    # Processing query
    query = "SELECT " + columns_list + " FROM {} WHERE " + search_column + " IN %s" + extra_where
    for i, table in enumerate(tables):
        data, _ = execute_query_env(
            conn_id, query=query.format(table), params=[tuple(search_list_filter)],
            label='SELECT FROM {}; count = {}'.format(table, len(search_list_filter)))
        for row in data:
            if single_value:
                set_result(row[search_column_id], row[0])
            else:
                if search_in_columns:
                    set_result(row[search_column_id], row)
                else:
                    set_result(row[search_column_id], row[:search_column_id] + row[search_column_id + 1:])

        if i < len(tables) - 1:
            if dict_result:
                search_list_filter -= result.keys()
            else:
                search_list_filter.clear()
                if unique or ordered:
                    for key in search_list:
                        if duplicates:
                            for idx in search_list[key]:
                                if result[idx] is default_obj:
                                    search_list_filter.append(key)
                        else:
                            if result[search_list[key]] is default_obj:
                                search_list_filter.append(key)
                else:
                    for key in search_list:
                        if not_found_keys[search_list[key]]:
                            search_list_filter.append(key)
            if not search_list_filter:
                break

    # Preparing results
    if dict_result:
        search_list_filter -= result.keys()
        if search_list_filter:
            if raise_not_found:
                str_not_found_keys = [str(x) for x in search_list_filter]
                if len(str_not_found_keys) > 20:
                    str_not_found_keys = \
                        ', '.join(str_not_found_keys[:20]) + ', ... total {} keys'.format(len(str_not_found_keys))
                else:
                    str_not_found_keys = ', '.join(str_not_found_keys)
                raise ValueError('Values for ' + search_column + ' IN (' + str_not_found_keys + ') not found!')
            elif not dict_exclude_not_found:
                if unique:
                    result.update({key: default_value for key in search_list_filter})
                else:
                    result.update({key: [] for key in search_list_filter})
    elif unique:
        for key in search_list:
            if duplicates:
                for idx in search_list[key]:
                    if result[idx] is default_obj:
                        if raise_not_found:
                            raise ValueError('Value for ' + search_column + '=' + str(key) + ' not found!')
                        result[idx] = default_value
            else:
                if result[search_list[key]] is default_obj:
                    if raise_not_found:
                        raise ValueError('Value for ' + search_column + '=' + str(key) + ' not found!')
                    result[search_list[key]] = default_value
    elif ordered:
        new_result = []
        for values in result:
            if values is not default_obj:
                for value in values:
                    new_result.append(value)
        # Cython workaround: can not delete variable 'result' referenced in nested scope
        # TODO::DEBUG can be memory leak - TEST IT!
        result[:] = new_result

    # Cython workaround: can not delete variable 'search_list' referenced in nested scope
    # TODO::DEBUG can be memory leak - TEST IT!
    if isinstance(search_list, list):
        search_list[:] = []
    else:
        search_list.clear()
    del search_list_filter

    return result


@timed_statement
def copy_from_io_ex(
        conn_id: str, table: str, string_io: IO, label: str = None, columns: Iterable[str] = None,
        commit: bool = False, delimiter: str = ';'):
    """
    Inserts data to DB from stream
    !WARNING! Funcion DOES NOT close the stream

    :param conn_id: connection id
    :param table: table name in format: <schema_name>.<table_name>
    :param string_io: source stream with data (rewind is not neccesary)
    :param label: debug label
    :param columns: columns list (only if rows in stream does not contains all columns)
    :param delimiter: columns delimiter
    :param commit: commit flag. True if force commit needed
    """
    debug(label or 'COPY {}'.format(table))
    copy_start = time()
    if columns is None:
        query = "COPY {} FROM STDIN WITH (FORMAT csv, DELIMITER '{}')".format(table, delimiter)
    else:
        query = "COPY {} ({}) FROM STDIN WITH (FORMAT csv, DELIMITER '{}')".format(table, ','.join(columns), delimiter)

    string_io.seek(0)
    _, error_text = execute_copy(conn_id, query=query, src=string_io, commit=commit)
    if error_text:
        error(error_text)

    debug('done in {}'.format(elapsed_str(copy_start)))


def move_from_new(
        conn_id: str, schema_name: str, table_name: str, where: str, columns: Iterable[str] = None, label: str = None
) -> bool:
    """
    Moves rows from `new_<table_name>` to `<table_name>`

    :param conn_id: connection id
    :param schema_name: schema name
    :param table_name: table name (without `new_` prefix)
    :param where: where clause (string, without WHERE statement)
    :param columns: columns list, optional
    :param label: debug label, optional
    :return: True if success
    """
    rules = (('{}.new_{}'.format(schema_name, table_name), '{}.{}'.format(schema_name, table_name)),)
    return move_data_to(conn_id, rules, where, columns, label)


def move_data_to(
        conn_id: str, move_rules: Iterable[Tuple[str, str]], where: str,
        columns: Iterable[str] = None, label: str = None
) -> bool:
    """
    Moves data from one table to another

    :param conn_id: connection id
    :param move_rules: move rules in format: [(<from_table>, <to_table>), ...]
    :param where: where clause (string, without WHERE statement)
    :param columns: columns list, optional
    :param label: debug label, optional
    :return: True if success
    """
    sel_columns = ', '.join(columns) if columns else '*'
    ins_columns = ('(' + sel_columns + ')') if columns else ''

    for from_table, to_table in move_rules:
        debug(label or 'MOVE from {} to {}'.format(from_table, to_table))
        copy_start = time()
        query = """
            INSERT INTO {} {} (SELECT {} FROM {} WHERE {});
            DELETE FROM {} WHERE {};
        """
        query = query.format(to_table, ins_columns, sel_columns, from_table, where, from_table, where)
        execute_query_env(conn_id, query=query, fetch_result=False)

        debug('done in {}'.format(elapsed_str(copy_start)))
    return True


def move_table_to_schema(conn_id: str, table: str, schema: str):
    """
    Moves table from one schema to another

    :param conn_id:
    :param table: table name in format: <schema_name>.<table_name>
    :param schema: destination schema name
    """
    query = "ALTER TABLE {} SET SCHEMA {}".format(table, schema)
    execute_query_env(conn_id, query=query, fetch_result=False)


def copy_from_local_file(
        conn_id: str, filename: str, schema_name: str, table_name: str, columns: Iterable[str] = None,
        commit: bool = False, separator: str = ',', chunk_size: int = 100000, mix: Union[Dict, List] = None,
        cast: Union[Dict, List] = None, validator: callable = None, custom: callable = None, catch_raise: bool = True
):
    """
    Executes COPY from local file to remote postgres
    :param conn_id:
    :param filename:
    :param schema_name:
    :param table_name:
    :param columns: list
    :param commit: True - commit after last copy
    :param separator: substring between csv-values
    :param chunk_size: max count of lines loaded to StringIO between COPYs
    :param mix: dict or list of indexes {table_col_index: file_col_index, ...} (must be the same length as columns)
            example: swap 2 fields: max={0:1, 1:0}
                     place 3rd field on 1st place: max={0:2, 1:0, 2:1}
    :param cast: dict or list of callable-s (or None-s if typecast not needed) with 1 argument and 1 result
            example: {0: round, 1: lambda x: datetime.utcfromtimestamp(x), 2: None}
            warning: if callable raises exception ValueError - current line is skipped
    :param validator: function with len(columns) positional arguments, which decides current line is valid or not
            (if it returns False or raises ValueError - line skipped)
    :param custom: function with (number of columns in csv) positional arguments
            returns tuple of len(columns) elements (return None or raise ValueError to skip line)
            if `custom` is not None it replaces min, cast and validator
    :param catch_raise: if False - don't catch ValueError
    """
    table = '{}.{}'.format(schema_name, table_name) if schema_name else table_name

    f = open(filename, 'r')
    io = StringIO()
    io_lines = 0
    line = True
    while line:
        line = f.readline()
        if not line:
            break
        line = line[:-1].split(separator)
        try:
            if custom is not None:
                line = custom(line)
                if line is None:
                    continue
            else:
                line = [line[idx] for idx in mix] if mix is not None else line
                line = [cast[idx](val) if cast[idx] is not None else val for idx, val in enumerate(line)] \
                    if cast is not None else line
                if validator is not None:
                    if not validator(*line):
                        continue
        except ValueError as e:
            if catch_raise:
                continue
            else:
                raise e
        write_to_io(io, *line)
        io_lines += 1
        if io_lines >= chunk_size:
            copy_from_io_ex(conn_id, table, io, columns=columns)
            io_lines = 0
            io.close()
            io = StringIO()

    if io_lines > 0:
        copy_from_io_ex(conn_id, table, io, columns=columns, commit=commit)
    else:
        do_commit(conn_id) if commit else None
    io.close()


def get_dependent_view_list(conn_id: str, table_name: str) -> List[str]:
    """
    Returns list of dependent views by specified table

    :param conn_id:
    :param table_name: table name in format: <schema_name>.<table_name> OR <table_name> (if table in public schema)
    :return: dependent views
    """
    query = """
        WITH RECURSIVE view_deps AS (
        SELECT DISTINCT dependent_ns.nspname as dependent_schema
        , dependent_view.relname as dependent_view
        , source_ns.nspname as source_schema
        , source_table.relname as source_table
        FROM pg_depend
        JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
        JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
        JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
        JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
        JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
        WHERE NOT (dependent_ns.nspname = source_ns.nspname AND dependent_view.relname = source_table.relname)
        UNION
        SELECT DISTINCT dependent_ns.nspname as dependent_schema
        , dependent_view.relname as dependent_view
        , source_ns.nspname as source_schema
        , source_table.relname as source_table
        FROM pg_depend
        JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
        JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
        JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
        JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
        JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
        INNER JOIN view_deps vd
            ON vd.dependent_schema = source_ns.nspname
            AND vd.dependent_view = source_table.relname
            AND NOT (dependent_ns.nspname = vd.dependent_schema AND dependent_view.relname = vd.dependent_view)
        )

        SELECT dependent_schema, dependent_view
        FROM view_deps
        WHERE source_schema = %s AND source_table = %s
        ORDER BY dependent_schema, dependent_view;
    """

    tmp = table_name.split('.')
    if len(tmp) > 1:
        schema_name, table_name = tmp[0], tmp[1]
    else:
        schema_name, table_name = 'public', table_name

    result, _ = execute_query_env(conn_id, query=query, params=[schema_name, table_name])
    result = ['{}.{}'.format(dependent_schema, dependent_view) for dependent_schema, dependent_view in result]
    return result


def update_many(
        conn_id: str, table: str, columns: Union[Tuple[str], List[str]], keys: List[str],
        values: Union[List[tuple], Tuple[tuple]], cast: Dict[str, str] = None, returning: Iterable[str] = None,
        returning_map: Callable = None, many_page_size: int = 2500
) -> TExecuteQueryEnvResult:
    """
    Updates many rows with unique values via VALUES
    !WARNING! Columns from `keys` cannot be updated - its values uses as a search keys
    example:
        UPDATE datamart.entity e SET n_tx = v.n_tx, in_tx = v.in_tx
        FROM (SELECT * FROM (VALUES %s) as t (id, n_tx, in_tx)) v
        WHERE e.id = v.id

    :param conn_id:
    :param table: table name in format: <schema_name>.<table_name>
    :param columns: columns list (in appropriate order to values)
    :param keys: columns list for WHERE (search keys), subset of columns
    :param values: rows data
    :param cast: templates for values typecasting: {column: str_sql, ...}, optional
    :param returning: columns for RETURNING, optional
    :param returning_map: Model class method, which returns column mapping for RETURNING
    :param many_page_size: count of rows for update in one query
    :return: execute_query_env result
    """
    query = """
        UPDATE {} d SET {}
        FROM (SELECT {} FROM (VALUES %s) as t ({})) v
        WHERE {}
    """
    if set(keys) & set(columns) != set(keys):
        raise RuntimeError("keys must be a subset of columns")
    if len(columns) == len(keys):
        raise RuntimeError("columns must contain at least one non-key")

    assigns = ', '.join(['{0} = v.{0}'.format(col) for col in columns if col not in keys])
    where_str = ' AND '.join(['d.{0} = v.{0}'.format(col) for col in keys])
    casts = ','.join('{} as {}'.format(cast.get(col, col), col) for col in columns) if cast else '*'
    query = query.format(table, assigns, casts, ', '.join(columns), where_str)

    if returning:
        sql_map = returning_map(alias='d') if returning_map else {}
        query += f" RETURNING {', '.join([sql_map.get(col, f'd.{col}') for col in returning])}"

    return execute_query_env(
        conn_id, query=query, insert_many=tuple(values), many_page_size=many_page_size,
        fetch_result=bool(returning))


def insert_many_simple(conn_id: str, table: str, fields: List[str], values: List[Tuple]):
    """
    DEPRECATED Inserts many rows via one query
    :param conn_id:
    :param table: table name in format: <schema_name>.<table_name>
    :param fields: columns list in appropriate order to insert data
    :param values: rows list to insert; each row contains column values in appropriate order to `fields`
    """
    from backend_tools.misc import deprecation
    deprecation.warn('Use `insert_many` instead of this function')
    query_insert = "INSERT INTO {} ({}) VALUES %s".format(table, ','.join(fields))
    execute_query_env(conn_id, query=query_insert, fetch_result=False, insert_many=values)


def insert_many(
        conn_id: str, table: str, fields: Union[List[str], Tuple[str]], values: Union[List[Tuple], Tuple[Tuple]],
        cast: Dict = None, on_conflict: str = None, returning: str = None, batch_size: int = 100
) -> TExecuteQueryEnvResult:
    """
    Inserts many rows via batched queries

    :param conn_id:
    :param table: table name in format: <schema_name>.<table_name>
    :param fields: columns list in appropriate order to insert data
    :param values: rows list to insert; each row contains column values in appropriate order to `fields`
    :param cast: templates for values typecasting
    :param on_conflict: ON CONFLICT clause (without statement), optional
    :param returning: RETURNING columns list, optional
    :param batch_size: count of rows for update in one query
    :return: execute_query_env result
    """
    query_insert = """
        WITH v({fields}) AS (VALUES %s)
        INSERT INTO {tbname} ({fields})
        SELECT {fields_casted} FROM v
        {on_conflict_clause} {returning_clause}
    """
    cast = cast or {}
    fields_casted = ','.join([cast.get(f, f) for f in fields])
    on_conflict_clause = 'ON CONFLICT {}'.format(on_conflict) if on_conflict else ''
    returning_clause = 'RETURNING {}'.format(returning) if returning else ''
    res = execute_query_env(
        conn_id, query=query_insert.format(
            tbname=table, fields=','.join(fields), fields_casted=fields_casted,
            on_conflict_clause=on_conflict_clause, returning_clause=returning_clause),
        fetch_result=bool(returning), insert_many=values, many_page_size=batch_size)
    return res


def batch_execute_query_env(
        conn_id: str, query: str,
        batch_param: Iterable = None, batch_size: int = 2500, batch_key: Union[int, str] = -1,
        params: Union[List, Dict] = None, return_columns: bool = False, **kwargs
) -> Union[TExecuteQueryEnvResult, Any]:
    """
    Batched query execution

    Example:
        result = batch_execute_query_env(
            conn_id, "SELECT id, address FROM blockchain.address WHERE id IN %(ids)s AND time >= %(time)s",
            params={'ids': address_ids, 'time': '2017-01-01'}, batch_key='ids')

    IMPORTANT: <iterable> must be one of the following types: list, set, tuple, dict_keys or dict_values

    :param conn_id:
    :param query: SQL query
    :param batch_param:
        values which must be splitted in batches (optional)
        If None - `batch_param` retrieves from `params[batch_key]`
            batch_execute_query_env(conn_id, query, batch_param=<iterable>) ===
            batch_execute_query_env(conn_id, query, params=[<iterable>], batch_key=0) ===
            batch_execute_query_env(conn_id, query, params={'ids': <iterable>}, batch_key='ids')
        !WARNING! `batch_param` must contain unique values
    :param batch_size: size of batch for one query
    :param batch_key: index/key in `param` to retrieve `batch_param` (optional, default -1)
    :param params: parameters for query, must contain element with `batch_key` index/key (optional)
        IMPORTANT: if using defaults for `batch_param`, `batch_key` - <iterable> must be the last in the `params`
    :param return_columns: flag for add columns in result
    :return: execute_query_env result
    """
    tmp = batch_param
    if batch_param is None:
        if isinstance(batch_key, int) and batch_key < 0:
            batch_key += len(params)

        tmp = params[batch_key]
        if isinstance(params, list):
            params_batch = [v if idx != batch_key else None for idx, v in enumerate(params)]
        else:
            params_batch = {k: v for k, v in params.items() if k != batch_key}

    # Allowed types check
    if not any([isinstance(tmp, x) for x in [list, set, tuple, KeysView, ValuesView]]):
        raise RuntimeError('<iterable> must be list, set, tuple, dict_keys or dict_values')

    if not tmp:
        # empty value list
        # TODO::REFACTOR change result if return_columns=True
        return []

    tmp_del = False
    if not isinstance(tmp, list):
        tmp_del = True
        tmp = list(tmp)

    fetch_result = kwargs.get('fetch_result', True)

    result = []
    columns = []
    for i in range(0, len(tmp), batch_size):
        current_batch = tuple(tmp[i: i + batch_size])
        if batch_param is None:
            params_batch[batch_key] = current_batch
        else:
            params_batch = [current_batch]
        rows, columns = execute_query_env(conn_id, query=query, params=params_batch, **kwargs)
        result.extend(rows) if fetch_result else None
        del rows
    del params_batch
    if tmp_del:
        del tmp

    return (result, columns) if return_columns else result


def select_search_dict(
        conn_id: str, tables: List[str], columns: List[str], search_column: str, search_list: Iterable,
        raise_not_found: bool, batch_size=2500, where_lambda: callable = None
) -> Dict[Union[int, str], Tuple]:
    """
    Customizable (batched) search in multiple tables with result aggregation
    Helper for `aggregate_filter_select`

    :param conn_id:
    :param tables: list of table names
    :param columns: wanted columns
    :param search_column: column for search
    :param search_list: values list for search (ids, hashes etc), will be splitted to batches
    :param raise_not_found: flag for raising ValueError if at least one of `search_list` values was not found
    :param batch_size: size of batch for one query
    :param where_lambda: additional where clause
    :return: dict in format: {search_list_value: columns_values, ...}
        for every search value corresponds only one search result (from the last table)
    """
    result = {}
    tmp = search_list if isinstance(search_list, list) else list(search_list)
    extra_where = mogrify(conn_id, *where(where_lambda)) if where_lambda is not None else None
    for i in range(0, len(search_list), batch_size):
        data = aggregate_filter_select(
            conn_id, columns=columns, tables=tables,
            search_column=search_column, search_list=tmp[i: i + batch_size],
            raise_not_found=raise_not_found, dict_result=True, dict_exclude_not_found=True, extra_where=extra_where)
        result.update(data)
        del data
    return result


def copy_from_rows(
        conn_id: str, table: str, rows: Union[Tuple, List], label: Optional[str] = None,
        columns: Optional[Iterable[str]] = None, commit: bool = False, convert: bool = True
):
    """
    Helper for `copy_from_io_ex`, works with iterable sources
    See `copy_from_io_ex` for parameter details

    :param conn_id:
    :param table: 
    :param rows: data rows for insertion
    :param label: 
    :param columns: 
    :param commit: 
    :param convert: flag for forced lines escaping.
        For speed up can be set to False in case if the `rows` has no data with types: str, None
    """
    # if rows is `np.array`, then `not rows` can cause an exception
    if rows is None or len(rows) == 0:
        do_commit(conn_id) if commit else None
        return

    with StringIO() as io:
        if convert:
            io.write('\n'.join([';'.join([to_csv_value(col) for col in row]) for row in rows]))
        else:
            io.write('\n'.join([';'.join([str(col) for col in row]) for row in rows]))
        copy_from_io_ex(conn_id, table=table, string_io=io, label=label, columns=columns, commit=commit)


def copy_from_rows_batched(
        conn_id: str, table: str, rows: Union[Tuple, List], columns: Optional[Iterable[str]] = None,
        commit: bool = False, convert: bool = True, batch_size=100000
):
    """
    helper for `copy_from_rows` - workaround for
        psycopg2.errors.ProgramLimitExceeded: out of memory
        DETAIL:  Cannot enlarge string buffer containing 0 bytes by 1155638963 more bytes.
    """
    if len(rows) <= batch_size:
        # no need to batch
        copy_from_rows(conn_id, table, rows, columns=columns, commit=commit, convert=convert)
        return
    for i in range(0, len(rows), batch_size):
        rows_chunk = rows[i: i + batch_size]
        copy_from_rows(conn_id, table, rows_chunk, columns=columns, commit=False, convert=convert)
    do_commit(conn_id) if commit else None


def create_type_if_not_exists(conn_id: str, schema_name: str, type_name: str, definition: str):
    """ Helper for creating a new type. PG does not support `CREATE TYPE IF NOT EXISTS` """
    query = f"""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM pg_type 
                WHERE typname = lower('{type_name}') 
                AND typnamespace = (select oid from pg_namespace where nspname = lower('{schema_name}'))
            ) THEN 
                CREATE TYPE {schema_name}.{type_name} AS {definition};
            END IF;
        END
        $$;
    """
    execute_query_env(conn_id, query, fetch_result=False)
