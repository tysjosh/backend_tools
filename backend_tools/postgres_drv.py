"""
PostgreSQL driver
"""
import re
from typing import Union, Tuple, List, Any, Dict, Iterable, Set
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_REPEATABLE_READ, QueryCanceledError
from psycopg2.extras import execute_values

from .configuration import cfg_value
from .misc import RE_ALPHA_DIGIT_, get_current_script_filename, DatabaseStatementTimeout

# Avoids warning of `unused import`
ISOLATION_LEVEL_REPEATABLE_READ = ISOLATION_LEVEL_REPEATABLE_READ
ISOLATION_LEVEL_AUTOCOMMIT = ISOLATION_LEVEL_AUTOCOMMIT


PG_WITHOUT_COMMIT = cfg_value('PG_WITHOUT_COMMIT', cast=bool, default=False)

# The delay between cycles awaiting query result (for MQ based on PostgreSQL only)
#   IMPORTANT: it can`t be too small (to prevent redundant PG loading)
PG_REQWAIT_LOOP_DELAY = cfg_value('PG_REQWAIT_LOOP_DELAY', cast=float, default=0.5)


# Connections settings dict. Format: {<connection_id>: {<connection_params>}}
_configuration = {}
# Connection objects dict (connection pool). Format: {<connection_id>: <connection_object>}
_connections = {}
# Event channels dict, for re-subscription, in case of connection is lost (for MQ based on PostgreSQL only)
#   Format: {<connection_id>: [<channel_name_1>, ... <channel_name_N>]}
_channels = {}


class CustomConnectionContext:
    """
    Context manager for custom connection creation.
    Implements autoclose connection on exit from context.
    DOES NOT commit the changes
    """
    def __init__(self, conn_id, **kwargs):
        self.conn_id = conn_id
        self.kwargs = kwargs

    def __enter__(self):
        register_default_pg_connection(self.conn_id, **self.kwargs)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        close_connection(self.conn_id)


class AutocommitConnectionContext(CustomConnectionContext):
    """ Context for connection with autocommit (helper for `CustomConnectionContext`) """
    def __init__(self, conn_id, **kwargs):
        kwargs.pop('conn_level', None)
        super(AutocommitConnectionContext, self).__init__(conn_id, isolation_level=ISOLATION_LEVEL_AUTOCOMMIT, **kwargs)


class PGStatementTimeout(DatabaseStatementTimeout, QueryCanceledError):
    """ Query timeout error """
    pass


def __db_method(method):
    """
    Decorator for functions that performs DB queries. Implements silent error handling.
    Arguments for the decorated function:
        1st argument always must be `conn_id` - it will be replaced on corresponding connection object
        :fail_result: If None - exception will be raised.
            Otherwise - exception will be handled, `fail_result` will be returned as a result
    :return: (<result>, <error>)
        <result> - result of decorated function OR None if there was exception
        <error> - error text if there was exception, None otherwise
    """
    def core(conn_id, **kwargs):
        fail_result = kwargs.pop('fail_result', None)

        if fail_result is not None:
            # Handling the exception, return fail_result
            try:
                conn = _connect_to_db(conn_id)
                return method(conn, **kwargs), None

            except Exception as e:
                err = '{}: {}'.format(e.__class__.__name__, str(e))
                _close_connection(conn_id)
                return fail_result, err
        else:
            # Do not handling the exception
            conn = _connect_to_db(conn_id)
            result = method(conn, **kwargs), None
            return result

    return core


def _connect_to_db(conn_id: str):
    """
    Creates new connection object OR gets it from a pool.

    In case of create object performs re-subscribe to event channels
    :param conn_id: connection id
    :return: connection object
    """
    conn = _connections.get(conn_id)
    if conn:
        return conn

    cfg = dict(_configuration[conn_id])
    iso_level = cfg.pop('isolation_level', None)
    statement_timeout = cfg.pop('statement_timeout', None)
    conn = psycopg2.connect(**cfg)
    if not conn:
        return conn

    if iso_level is not None:
        conn.set_isolation_level(iso_level)
    if statement_timeout is not None:
        _set_statement_timeout(conn, timeout=statement_timeout)
    _connections[conn_id] = conn

    # re-subscribing to event channels (for reconnect case)
    for channel in _channels.get(conn_id, []):
        _add_event_listener(conn_id, channel)
    return conn


def _add_connection(**kwargs):
    """
    Adds new connection settings.
    !!! WARNING !!! kwargs must have only keys with values (not keys with None)
    """
    conn_id = kwargs.pop('conn_id')
    _configuration[conn_id] = kwargs


def _get_event_connection_id(conn_id: str) -> str:
    """ Returns connection id for event channel by given `conn_id` """
    event_conn_id = conn_id + '_event'
    if event_conn_id not in _configuration:
        event_conn_params = dict(_configuration[conn_id])
        event_conn_params.update({'isolation_level': ISOLATION_LEVEL_AUTOCOMMIT})
        _add_connection(conn_id=event_conn_id, **event_conn_params)
    return event_conn_id


def _close_connection(conn_id: str):
    """ Closes connection """
    conn = _connections.pop(conn_id, None)
    if conn:
        try:
            conn.close()
        except Exception:
            pass


def _add_event_listener(conn_id: str, channel: str):
    """ Subscribes to the specified event channel """
    channel = re.sub(RE_ALPHA_DIGIT_, "", channel)
    query = "LISTEN {};".format(channel)
    return execute_query_light(conn_id, query=query, fail_result=False)


@__db_method
def _get_events(conn, all_events: bool = False):
    """
    Reads new events from the connection object
    Returns list of events in format: [(pid, channel, payload), ...]
    """
    events = []
    repeat = True
    while repeat:
        repeat = False
        conn.poll()
        while conn.notifies:
            notify = conn.notifies.pop(0)
            events.append((notify.pid, notify.channel, notify.payload))
            repeat = all_events

    return events


@__db_method
def execute_query_light(conn, query: str = '', params: Union[List, Dict] = None):
    """
    Light version of `execute_query`. DOES NOT return a query result

    :param conn: connection object
    :param query: sql query
    :param params: query params (in psycopg format)
    :return: True
    """
    with conn.cursor() as cur:
        cur.execute(query, params)
    return True


@__db_method
def execute_query(
        conn, query=None, params=None, commit=False, rollback=False, fetch_result=True,
        single_row=False, single_value=False, insert_many=None, many_page_size=100
) -> Tuple[Any, List]:
    """
    Performs query to DB

    :param conn: connection object
    :param query: sql query
    :param params: query params (in psycopg2 format)
    :param commit: flag for commit transaction after query
    :param rollback: flag for rollback transaction after query (only if commit != True)
    :param fetch_result: flag for fetching query result
    :param single_row: flag for returning one-row result (see :return:)
    :param single_value: flag for returning one-value result, has higher priority than `single_row` (see :return:)
    :param insert_many: data for INSERT/UPDATE multiple rows. in that case params ignored.
        Uses psycopg2 `execute_values()`
        data must be in format: ((value1_1, ..., value1_N), ... (valueM_1, ..., valueM_N))
        if omit - performs `cur.execute()`
    :param many_page_size: max row count per one INSERT statement (`page_size` arg for `execute_values()`)
        Uses with `insert_many` only
    :return: query result in format: (<data>, <columns>)
        <data> can be:
            - `fetchall()` result
            - data[0], if single_row=True
            - data[0][0], if single_value=True
            - True, if fetch_result=False
        <columns> - requested column list
    """
    data, columns = None, None
    with conn.cursor() as cur:
        if query:
            result_many = None
            if not insert_many:
                cur.execute(query, params)
            else:
                result_many = execute_values(cur, query, insert_many, page_size=many_page_size, fetch=fetch_result)

            if fetch_result and not insert_many:
                data = cur.fetchall()
                columns = [d.name for d in cur.description]

                if data:
                    # TODO::TECHDEBT known issue: if query returns an empty result - data = []
                    #   for `single_value` and `single_row` its untypical result, need fix, but:
                    #       - returning None for `single_value` is not allowed - query result can be `null`
                    #       - result for `single_row` must be tuple() or None
                    if single_value and data[0]:
                        data = data[0][0]
                    elif single_row:
                        data = data[0]
            elif fetch_result and insert_many:
                data = result_many
                columns = [d.name for d in cur.description]
            else:
                data = True

        if commit and not PG_WITHOUT_COMMIT:
            conn.commit()
        elif rollback:
            conn.rollback()

    return data, columns


@__db_method
def execute_copy(conn, query: str = '', src=None, size: int = -1, commit: bool = False):
    """
    Performs query like "COPY ... FROM STDIN ..."

    :param conn: conection object
    :param query: sql query
    :param src: read/write object (file or StringIO)
    :param size: buffer size for read/write, in bytes
    :param commit: flag for commit after query
    :return: True
    """
    with conn.cursor() as cur:
        cur.copy_expert(query, src, size)
    if commit and not PG_WITHOUT_COMMIT:
        conn.commit()

    return True


@__db_method
def set_statement_timeout(conn, timeout: Union[str, int]):
    """ see `_set_statement_timeout()` """
    _set_statement_timeout(conn, timeout)


def _set_statement_timeout(conn, timeout: Union[str, int]):
    """
    Sets query execution timeout for current connection.
    Timeout settings preserves before connection closed

    :param conn: connection object
    :param timeout: 60 == '60s', 300 == '300s' == '5min', (0 == '0s' - no timeout, infinite query)
    """
    timeout = (str(timeout) + 's') if isinstance(timeout, int) else timeout
    conn.cursor().execute("SET statement_timeout = %s", [timeout])


def get_connection(conn_id: str):
    """
    Interface for creating/getting a connection object from the pool by a given connection id

    :param conn_id: connection id
    :return: connection object
    """
    return _connect_to_db(conn_id)


def reset_pools():
    """ Clears connections settings and closes all connections in the pool """
    _configuration.clear()
    reset_connections()
    _channels.clear()


def reset_connections():
    """ Closes all connections in the pool """
    for conn_id in list(_connections.keys()):
        try:
            close_connection(conn_id)
        except Exception:
            pass
    _connections.clear()


def add_connection(
        conn_id: str, host: str, port: Union[str, int], dbname: str, user: str, password: str = None,
        isolation_level: int = None, application_name: str = None, statement_timeout: Union[str, int] = None):
    """
    Adding settings for new connection to the connection settings dict.
    Function can be called multiple times with the same settings for the same connection ID.
        But raises an exception on attempt to add new settings for existing connection ID.
    DOES NOT establishes connection, it will be established on the first query to DB.

    :param conn_id: connection ID
    :param host: db host
    :param port: db port
    :param dbname: db name
    :param user: db user
    :param password: db user password
    :param isolation_level:
    :param application_name:
    :param statement_timeout:
    """
    kwargs = {
        'host': host,
        'port': port,
        'dbname': dbname,
        'user': user,
        'password': password,
        'isolation_level': isolation_level,
        'statement_timeout': statement_timeout,
        'application_name': application_name
    }
    kwargs = {k: v for k, v in kwargs.items() if v is not None}
    if conn_id in _configuration:
        # checks if settings has difference
        if _configuration[conn_id] != kwargs:
            raise ValueError('Connection with id "{}" is already in pool'.format(conn_id))
        # all settings are the same => no need to add settings or raise an exception
        return

    _add_connection(conn_id=conn_id, **kwargs)


def close_connection(conn_id: str, event_conn: bool = False):
    """
    Interface for closing a connection

    :param conn_id: connection ID
    :param event_conn: flag for forced close events channels connection
    """
    _close_connection(conn_id)
    _close_connection(_get_event_connection_id(conn_id)) if event_conn else None


def terminate_connection(conn_id: str, event_conn: bool = False):
    """ DEPRECATED """
    from backend_tools.misc import deprecation
    deprecation.warn('This function is deprecated. Use `pg_forget_connection` instead of this', DeprecationWarning)
    pg_forget_connection(conn_id, event_conn=event_conn)


def pg_forget_connection(conn_id: str, event_conn: bool = False):
    """
    Removes connection and its settings from the pool. Closes the connection if it was open

    :param conn_id: connection ID
    :param event_conn: flag for forced close events channels connection
    """
    close_connection(conn_id, event_conn)
    del _configuration[conn_id]


def get_autocommit_connection_id(conn_id: str) -> str:
    """
    Creates a copy of connection settings (by given `conn_id`) but with ISOLATION_LEVEL_AUTOCOMMIT.
    Returns a new connection id

    :param conn_id: source connection ID
    :return: autocommit connection ID
    """
    return _get_event_connection_id(conn_id)


def add_event_listener(conn_id: str, channel: str) -> Tuple[bool, Any]:
    """
    Interface for subscribing to the event channel

    :param conn_id: connection ID
    :param channel: event channel name
    :return: True, if subscribing is successful
    """
    conn_id = _get_event_connection_id(conn_id)
    _channels[conn_id] = list(set(_channels.get(conn_id, []) + [channel]))
    return _add_event_listener(conn_id, channel)


def remove_event_listener(conn_id: str, channel: str) -> Tuple[bool, Any]:
    """
    Interface for unsubscribing from the event channel

    :param conn_id: connection ID
    :param channel: event channel name
    :return: True, if unsubscribing is successful
    """
    conn_id = _get_event_connection_id(conn_id)
    new_conns = filter(lambda x: x != channel, _channels.get(conn_id, []))
    _channels[conn_id] = list(new_conns)
    channel = re.sub(RE_ALPHA_DIGIT_, "", channel)
    query = "UNLISTEN {};".format(channel)
    return execute_query_light(conn_id, query=query, fail_result=False)


def generate_event(conn_id: str, channel: str, data: str) -> Tuple[bool, Any]:
    """
    Interface for event generation on specified channel

    :param conn_id: connection ID
    :param channel: event channel name (event name)
    :param data: event data
        IMPORTANT: data size must be <= 7kb
    :return: True, if success
    """
    channel = re.sub(RE_ALPHA_DIGIT_, "", channel)
    query = "NOTIFY {}, %s;".format(channel)
    return execute_query_light(
        _get_event_connection_id(conn_id), query=query, params=[data], fail_result=False)


def get_events(conn_id: str, all_events: bool = False) -> Tuple[List, Any]:
    """
    Interface for reading all new events from the connection

    :param conn_id: connection ID
    :param all_events: if True - reading all new events, otherwise reads only one event per function call
    :return: event list in format: [(pid, channel, payload)]
    """
    return _get_events(
        _get_event_connection_id(conn_id), all_events=all_events, fail_result=[])


def adapt_types(types: Iterable):
    """ Interface for setting up the typecast rules (env for `psycopg2.extensions.register_adapter()`) """
    def _adapt_types(src_type):
        return psycopg2.extensions.AsIs(src_type)

    for entry in types:
        psycopg2.extensions.register_adapter(entry, _adapt_types)


def register_default_pg_connection(conn_id, application_name=get_current_script_filename(), **kwargs):
    """
    Adding new connection settings with defaults from configuration file
        allowed kwargs:
            isolation_level - ISOLATION_LEVEL_AUTOCOMMIT, ISOLATION_LEVEL_REPEATABLE_READ, etc from psycopg2.extensions
            statement_timeout - '60s', '5min', etc

    :param conn_id: connection ID
    :param application_name: current process label
    """
    defaults = {
        'conn_id': conn_id,
        'application_name': application_name,
        'host': DB_HOST,
        'port': DB_PORT,
        'dbname': DB_NAME,
        'user': DB_USER,
        'password': DB_PASS
    }
    defaults.update(kwargs)
    add_connection(**defaults)


def create_select_from_jsonb_path(key: Union[str, int, Iterable] = None) -> str:
    """
    Creates a path for selecting element from jsonb field by given `key`
    For example: key = [1, 'a', 2, ...], path: ->1->'a'->2...

    :param key: index/key for selecting (number, string, list)
    :return: created path
    """
    if key is None:
        path = ''
    elif isinstance(key, str):
        path = "->'{}'".format(key)
    elif isinstance(key, int):
        path = '->{}'.format(key)
    else:
        path = ''.join(["->'{}'".format(k) if isinstance(k, str) else '->{}'.format(k) for k in key])

    return '{}'.format(path)


def create_update_to_jsonb_path(key: Union[str, int, Iterable]):
    """
    Creates a path for updating jsonb field
        TODO::DOC add some examples

    :param key: index/key for updating (number, string, list)
    :return: path in format: '{1,a,2...}'
    """
    if isinstance(key, str):
        path = '{' + key + '}'
    elif isinstance(key, int):
        path = '{' + str(key) + '}'
    else:
        path = '{' + ','.join([str(k) for k in key]) + '}'

    return "'{}'".format(path)


def query_result_to_dict(columns: Iterable[str], rows: Iterable) -> List[Dict]:
    """
    Transforms each row of query result into Dict like:
        { 'col1': value1, ... , 'colN': valueN }

    :param columns: column names list (result of execute_query*)
    :param rows: query result data (result of execute_query*)
    :return: dict()
    """
    return [dict(zip(columns, row)) for row in rows or []]


def query_result_to_set(rows: Iterable) -> Set:
    """
    Transforms one-column query result into a set

    :param rows: query result data
    :return: set()
    """
    return {row[0] for row in rows}


def query_result_to_dict_by_column(rows: Iterable, col_num: int) -> Dict:
    """
    Transforms query result into dict, using `col_num` column values as a keys

    :param rows: query result data
    :param col_num: key column index
    :return: dict()
    """
    result = {}
    for row in rows:
        mut_row = list(row)
        key = mut_row.pop(col_num)
        if key not in result:
            result[key] = []
        result[key].append(mut_row)
    return result


def mogrify(conn_id: str, query: str, params: Dict) -> str:
    """
    Helper for applying parameters to a sql query (using `psycopg.mogrify()`)

    :param conn_id: connection ID
    :param query: sql query with parameter placeholders
    :param params: query parameters
    :return: query with inserted parameter values
    """
    return get_connection(conn_id).cursor().mogrify(query, params).decode()


# default credentials for DB connection
DB_HOST = cfg_value('DB_HOST', default='127.0.0.1')
DB_PORT = cfg_value('DB_PORT', default=5432)
DB_NAME = cfg_value('DB_NAME', default='datamart')
DB_USER = cfg_value('DB_USER', default='postgres')
DB_PASS = cfg_value('DB_PASS', default='postgres')