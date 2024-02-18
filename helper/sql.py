import re
from typing import Dict, List, Iterable, Union, Optional, Tuple

from backend_tools.postgres_drv import mogrify
from backend_tools.logger import info
from backend_tools.misc import deprecation
from backend_tools.sqlwerks import execute_query_env, create_schema
from helper.consts import SCHEMA_TEMP

from helper.typing import StrOrList, IntOrList

nulls_eq_0 = {
    'asc': 'NULLS FIRST',
    'desc': 'NULLS LAST'
}


def make_timestamp_extract(field, alias=None, with_as=False):
    deprecation.warn('Function is deprecated. Use MODEL_TEMPLATE.timestamp instead', DeprecationWarning)
    tbl_alias = alias + '.' if alias else ''
    field_alias = f'as {field}' if with_as else ''
    return f"extract(epoch from {tbl_alias}{field})::BIGINT {field_alias}"


def build_select_expr(
        sql_map_dict: Dict, fields: Iterable[str], as_list: bool = False, alias: str = None) -> Union[str, List[str]]:
    deprecation.warn('Function is deprecated. Use Model.sql_select_fields instead', DeprecationWarning)
    alias = alias + '.' if alias else ''
    list_expr = [alias + sql_map_dict.get(field, field) for field in fields]
    return list_expr if as_list else ','.join(list_expr)


def build_update_query(query, fields: Dict, nulls=False):
    update_keys = []
    for column, value in fields.items():
        if value is None and not nulls:
            continue
        update_keys.append(str(column) + ' = %(' + column + ')s')
    query_update = query.format(','.join(update_keys))
    return query_update


def delete_foreign_key(conn_id, column: StrOrList, key: Union[StrOrList, IntOrList], fetch: StrOrList = None) \
        -> Optional[Dict[str, List]]:
    # TODO::REFACTOR is an "executable" function, should be moved to backend-tools.
    # here is a place for low-level helpers without DB calls
    # Better yet, move the entire module to backend-tools
    """
    Deletes data from multiple tables by foreign key, optionally returning IDs of deleted rows

    :param conn_id:
    :param column: - fully qualified column name (schema, table, column) or a list of such names
    :param key: - value or values ​​to delete
    :param fetch: - the full name of the columns to be returned (if necessary)
    :return: If fetch is given, then returns Dict {column_name_from_fetch: list of values ​​from RETURNING}

    Example:
    Remove rows from activity_metric, activity_address, activity_snapshot with case_id=125
    At the same time, return RETURNING id from activity_metric, and return RETURNING id, address_id from activity_address.

    delete_foreign_key(
        conn_id,
        [
            'userdata.activity_metric.case_id',
            'userdata.activity_address.case_id',
            'usermart.activity_snapshot.case_id'
        ],
        125,
        [
            'userdata.activity_metric.id',
            'userdata.activity_address.id',
            'userdata.activity_address.address_id'
        ]
    )
    """
    query = "DELETE FROM {table} WHERE {colname} IN %s {fetch_part}"
    columns = [column] if isinstance(column, str) else column
    fetches = [fetch] if isinstance(fetch, str) else fetch
    keys = [key] if isinstance(key, str) or isinstance(key, int) else key

    returning = {fetch_key: [] for fetch_key in (fetches or [])}

    if not keys:
        return returning if fetch else None

    fetch_dict = {}
    for fetch_key in fetches or []:
        table, _, colname = fetch_key.rpartition('.')
        fetch_dict.setdefault(table, {}).update({colname: fetch_key})

    for col in columns:
        table, _, colname = col.rpartition('.')
        fetch_part = "RETURNING {}".format(','.join(fetch_dict[table].keys())) if table in fetch_dict else ''
        q = query.format(table=table, colname=colname, fetch_part=fetch_part)
        query_result, cols = execute_query_env(conn_id, q, params=[tuple(keys)], fetch_result=bool(fetch_part))
        if fetch_part:
            for colname, fetch_key in fetch_dict[table].items():
                returning[fetch_key] = [row[cols.index(colname)] for row in query_result]

    return returning if fetch else None


def update_foreign_key(conn_id, column: StrOrList, value_from, value_to):
    # TODO::REFACTOR это тоже в backend-tools
    # TODO::CREATE поддержка returning (когда понадобится)
    """
    Обновляет данные в множестве таблиц по внешнему ключу

    :param conn_id:
    :param column: - полное имя столбца (схема, таблица, столбец) или список таких имён
    :param value_from: значение column которое надо обновить
    :param value_to: новое значение column

    Пример:
    Обновить поле case_id в значение 666 в таблицах
    activity_metric, activity_address, activity_snapshot с case_id=125


    update_foreign_key(
        conn_id,
        [
            'userdata.activity_metric.case_id',
            'userdata.activity_address.case_id',
            'usermart.activity_snapshot.case_id'
        ],
        125,
        666
    )
    """
    query_tmpl = """
        UPDATE {table}
            SET {set_expr}
        WHERE {colname} = %(key)s
    """
    columns = [column] if isinstance(column, str) else column

    for col in columns:
        table, _, colname = col.rpartition('.')
        update_query_tmpl = query_tmpl.format(table=table, colname=colname, set_expr='{}')
        update_query = build_update_query(update_query_tmpl, {colname: value_to}, nulls=True)
        execute_query_env(conn_id, update_query, params={'key': value_from, colname: value_to}, fetch_result=False)


def check_table_exists(conn_id, schema, table):
    q_exists = """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_name = %s
          AND table_type = 'BASE TABLE'
    """
    exists, _ = execute_query_env(conn_id, q_exists, params=[schema, table], single_value=True)
    mogrify(conn_id, q_exists, params=[schema, table])
    return exists


def create_primary_key(conn_id, schema, table, by):
    q_check = """
        SELECT
            pg_index.indisvalid
        FROM
            pg_index
                JOIN pg_class cls ON cls.oid = pg_index.indrelid
                JOIN pg_class idx ON idx.oid = pg_index.indexrelid
                LEFT JOIN pg_namespace nsp ON nsp.oid = cls.relnamespace
        WHERE
            cls.relkind = ANY (ARRAY['r'::"char", 'm'::"char"]) AND
            idx.relkind = 'i'::"char" AND
            nsp.nspname=%(schema)s AND
            idx.relname=%(index)s
    """
    q_uniq = "CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS {index} ON {schema}.{table}({fields})"
    q_pk = """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints 
                WHERE constraint_schema='{schema}' AND table_name = '{table}' AND constraint_type='PRIMARY KEY'
            ) THEN
                ALTER TABLE {schema}.{table} ADD CONSTRAINT {pk} PRIMARY KEY USING INDEX {index};
            END IF;
        END
        $$;
    """
    if isinstance(by, list):
        index = f'{table}_{"_".join(by)}_pk'
        pk = index
        check_params = {'index': index, 'schema': schema}
        check, _ = execute_query_env(conn_id, q_check, params=check_params, single_value=True)
        if check is False:
            info(f'Drop invalid index {schema}.{index}')
            execute_query_env(conn_id, f"DROP INDEX {schema}.{index}", fetch_result=False)
        q_create_uniq = q_uniq.format(index=index, schema=schema, table=table, fields=','.join(by))
        info(f'Create uniq key for {schema}.{table}')
        execute_query_env(conn_id, q_create_uniq, fetch_result=False)
    else:
        index = by
        pk = '{}_pk'.format(re.sub('_(u|uniq_)?idx$', '', by))
    info(f'Create PK for {schema}.{table}')
    q_create_pk = q_pk.format(schema=schema, table=table, pk=pk, index=index)
    execute_query_env(conn_id, q_create_pk, fetch_result=False)


def create_temp_schema(conn_id):
    create_schema(conn_id, SCHEMA_TEMP)


def _parse_ch_enum(enum_type) -> Tuple[str, Dict]:
    """
    :param enum_type: Type with enum, for example Nullable(Enum8('deposit' = 0, 'withdrawal' = 1))
    :return: Tuple('Nullable(Enum8({}))', {'deposit': 0, 'withdrawal': 1})
    """
    re_type = r'^\s*((?P<open>([^(]+\()+)(?P<enum>[^)]+)(?P<close>\)+))\s*$'
    re_enum = r"^\s*\\*'(?P<name>[^\\]*)\\*'\s*=\s*(?P<int>\d+)\s*$"
    type_match = re.match(re_type, enum_type)
    if not type_match:
        raise ValueError("Cannot parse clickhouse enum type")
    _open, _enum, _close = type_match.group('open'), type_match.group('enum'), type_match.group('close')

    enum_values = _enum.split(",")
    enum_dict = {}
    for enum_one in enum_values:
        match = re.match(re_enum, enum_one.strip())
        enum_dict[match.group('name')] = int(match.group('int'))

    type_pattern = _open + '{}' + _close
    return type_pattern, enum_dict
