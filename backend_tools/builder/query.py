from typing import List, Dict, Tuple


def _build_where(where_clause: str) -> str:
    """ Хелпер для построения {where} """
    return ' WHERE {}'.format(where_clause) if where_clause else ''


def _build_returning(returning: List) -> str:
    """ Хелпер для построения {returning} """
    return ' RETURNING {}'.format(','.join(returning)) if returning else ''


def _build_order(order_by: List, desc: bool) -> str:
    """
    Хелпер для построения {order}
    WARNING: desc arg is DEPRECATED, use direction in `order_by`. example: order_by = ['f1 ASC', 'f2 DESC']
    """
    return ' ORDER BY {} {}'.format(', '.join(order_by), 'DESC' if desc else '') if order_by else ''


def _build_group(group_by: List) -> str:
    """ Хелпер для построения {group} """
    return ' GROUP BY {}'.format(','.join(group_by)) if group_by else ''


def query_select(
        table: str, columns: List, where_clause: str, order_by: List, limit: int, desc: bool,
        group_by: List = None, offset: int = 0
) -> str:
    """
    Builds SELECT query
    :param table: name of table
    :param columns: list with needed columns
    :param where_clause: filter expression (without WHERE statement)
    :param order_by: list of fields (with direction) for ORDER BY (if needed)
    :param limit: value for LIMIT (if needed)
    :param desc: [DEPRECATED] use direction in `order_by`. example: order_by = ['f1 ASC', 'f2 DESC']
    :param group_by: list of fields for GROUP BY (optional)
    :param offset: offset (optional)
    """
    columns_list = ','.join(columns)
    order_by = _build_order(order_by, desc)
    group_by = _build_group(group_by)
    limit = ' LIMIT {}'.format(limit) if limit else ''
    offset = ' OFFSET {}'.format(offset) if offset else ''
    return "SELECT {columns} FROM {table}{where_clause}{group}{order}{limit}{offset}".format(
        columns=columns_list, table=table, where_clause=_build_where(where_clause),
        group=group_by, order=order_by, limit=limit, offset=offset)


def query_delete(table: str, where_clause: str, returning: List = None) -> str:
    """
    Builds SELECT query
    :param table: name of table
    :param where_clause: filter expression (without WHERE statement)
    :param returning: list of columns for returning
    """
    return "DELETE FROM {table}{where}{returning}".format(
        table=table, where=_build_where(where_clause), returning=_build_returning(returning))


def query_update(
        table: str, values: Dict, where_clause: str, returning: List, force_null: bool = False
) -> Tuple[str, Dict]:
    """
    Builds UPDATE query. Returns empty query if there no values for set
    :param table: name of table
    :param values: values for set {<field_name>: <new_value>}
    :param where_clause: filter expression (without WHERE statement)
    :param returning: list of columns for returning
    :param force_null: force update NULL values
    :return: (<update_query>, <params_for_set>)
    """
    params_for_set = {}
    set_values = []
    for col, val in values.items():
        if val is None and not force_null:
            continue

        param_name = 'val_{}'.format(col)
        params_for_set[param_name] = val
        set_values.append('{} = %({})s'.format(col, param_name))

    if not set_values:
        return '', {}

    return "UPDATE {table} SET {values}{where}{returning}".format(
        table=table, values=','.join(set_values),
        where=_build_where(where_clause), returning=_build_returning(returning)), params_for_set
