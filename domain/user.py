from datetime import datetime
from typing import List, Dict, Optional, Tuple

from backend_tools.misc import get_salted_hash
from backend_tools.builder.where import where
from backend_tools.sqlwerks import insert_many, execute_query_env
from backend_tools.tabular import Tabular, query_iterator
from backend_tools.errors import ERR_OK

from helper.sql import build_select_expr
from helper.errors import ERR_USER_EMAIL_EXISTS
from datasource.user import USER_DEFAULT_FIELDS, select_users, TB_REGISTRATION, TB_USER, \
    USER_UNVERIFIED_FIELDS
from helper.consts import ROLE_USER
from model.user import User


def users_get(
        conn_id, fields: List[str] = None, where_lambda: callable = None, activated: bool = True) -> Dict[int, User]:
    table = TB_USER if activated else TB_REGISTRATION
    default_fields = USER_DEFAULT_FIELDS if activated else USER_UNVERIFIED_FIELDS
    not_allowed = set(fields or []) - set(default_fields)
    if not_allowed:
        raise ValueError(f'Fields {not_allowed} not allowed')
    if fields and 'id' not in fields:
        fields.append('id')
    users = {user.id: user for user in select_users(conn_id, fields, where_lambda, table=table).to_models(User)}
    return users


def user_get_by(conn_id, by_col, value, fields: List[str] = None, activated: bool = True) -> Optional[User]:
    if by_col == 'email':
        by_col = 'lower(email)'
        value = value.lower()
    users = users_get(conn_id, fields, where_lambda=lambda t: t[by_col] == value, activated=activated)
    if not users:
        return None
    _, user = users.popitem()
    return user


def user_add(conn_id, name, email, password, country, activity, company, phone) -> Optional[User]:
    cols = ['person_name', 'email', 'hash', 'country', 'activity', 'company', 'phone', 'created']
    user_row = [(name, email, get_salted_hash(password), country, activity, company, phone, datetime.utcnow())]
    rows, _ = insert_many(conn_id, TB_REGISTRATION, cols, user_row, returning='id')
    return User(id=rows[0][0], person_name=name, email=email) if rows else None


def approve_new_user(conn_id, user_id):
    where_expr, where_params = where(lambda x: x.id == user_id)
    query = f"""
        WITH new_user AS (
            DELETE FROM {TB_REGISTRATION} WHERE {where_expr}
            RETURNING person_name, email, hash, country, activity, company, phone, created, '{ROLE_USER}' as role,
             '{datetime.utcnow()}'::TIMESTAMP as activated_at
        )
        INSERT INTO {TB_USER} (person_name, email, hash, country, activity, company, phone, created, role, activated_at)
        SELECT * from new_user
    """
    execute_query_env(conn_id, query, params=where_params, fetch_result=False)


def build_users_lambda(filter_settings):
    name = filter_settings.get('name')
    email = filter_settings.get('email')
    company = filter_settings.get('company')
    return lambda x: \
        x.person_name.ilike(f'%{name}%' if name else None) & \
        x.email.ilike(f'%{email}%' if email else None) & \
        x.company.ilike(f'%{company}%' if company else None)


def _query_get_all_users(filter_settings):
    where_expr, where_params = where(build_users_lambda(filter_settings))

    full_user_cols = build_select_expr(User.sql_map(), USER_DEFAULT_FIELDS, as_list=True)
    full_user_query = f"SELECT {','.join(full_user_cols)} FROM {TB_USER} WHERE {where_expr}"

    unverified_user_cols = ['NULL as id']
    unverified_user_cols.extend([
        f'NULL AS {col}' if col not in USER_UNVERIFIED_FIELDS else col
        for col in USER_DEFAULT_FIELDS if col != 'id'])
    unverified_user_cols = build_select_expr(User.sql_map(), unverified_user_cols, as_list=True)
    unverified_user_query = f"SELECT {','.join(unverified_user_cols)} FROM {TB_REGISTRATION} WHERE {where_expr}"

    activated = filter_settings.get('activated')
    if activated is True:
        queries = [full_user_query]
    elif activated is False:
        queries = [unverified_user_query]
    else:  # None
        queries = [full_user_query, unverified_user_query]

    return ' UNION ALL '.join(queries), where_params


def filter_all_users(conn_id, filter_settings, offset, limit, order, direction) -> Tabular:
    order_by = f'ORDER BY {order} {direction}' if order else ''
    limit = f' LIMIT {limit}' if limit else ''
    offset = f' OFFSET {offset}' if offset else ''
    main_query, main_params = _query_get_all_users(filter_settings)
    query = f'SELECT * FROM ({main_query}) as users {order_by} {limit} {offset}'
    users = query_iterator(conn_id, query, params=main_params)
    return users


def count_all_users(conn_id, filter_settings) -> int:
    main_query, main_params = _query_get_all_users(filter_settings)
    query = f'SELECT count(email) as total FROM ({main_query}) as users'
    count = query_iterator(conn_id, query, params=main_params).single_row()
    return count.total if count else 0


def prepare_user_fields(conn_id, user_id, **source) -> Tuple[Optional[Dict], int]:
    """
    !!! Mutable args !!!
    :param conn_id:
    :param user_id:
    :param source: Dict with user fields. Mutable
    :return: Error code
    """
    password = source.pop('password', None)
    if password:
        source['hash'] = get_salted_hash(password)

    if 'email' in source:
        user = user_get_by(conn_id, 'email', source['email'], ['id'])
        if user and user.id != user_id:
            return None, ERR_USER_EMAIL_EXISTS

    return source, ERR_OK