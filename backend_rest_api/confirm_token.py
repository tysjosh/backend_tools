from datetime import datetime
from time import time

from backend_tools.configuration import cfg_value
from backend_tools.service.misc import create_schema

from backend_rest_api.tools import generate_token
from backend_tools.errors import ERR_OK, ERR_INVALID_PARAMS, add_errors
from backend_tools.sqlwerks import execute_query_env
from backend_rest_api.orm import AbstractORMObject

TOKEN_EMAIL_CONFIRM = 'ec'
TOKEN_PASSWORD_RESET = 'pr'
TOKEN_NEW_USER = 'nu'
TOKEN_LENGTH = 128

ERR_CONFIRM_TOKEN_NOT_FOUND = 120


ERRORS = {
    ERR_CONFIRM_TOKEN_NOT_FOUND: 'Confirm token not found',
}
add_errors(ERRORS)


class ConfirmToken(AbstractORMObject):
    def __init__(self, **kwargs):
        self._user_id = kwargs.pop('user_id', 0)
        self._token = kwargs.pop('token', '')
        self._type = kwargs.pop('type', '')
        self._created_at = kwargs.pop('created_at', 0)


def confirm_token_create(conn_id, user_id, token=None, token_type=TOKEN_EMAIL_CONFIRM, commit=True):
    if not token:
        token = generate_token(token_length=TOKEN_LENGTH)
    query = "INSERT INTO {}.confirm_token VALUES %s".format(DB_SCHEMA)
    created_at = datetime.utcfromtimestamp(time())
    execute_query_env(
        conn_id, query=query, params=[(user_id, token, token_type, created_at)], fetch_result=False, commit=commit)
    return ConfirmToken(user_id=user_id, token=token, type=token_type, created_at=created_at), ERR_OK


def confirm_token_get(conn_id, user_id=None, token=None, token_type=None, single=False):
    """
    Sample of tokens according to well -known data. 
    If Single = True, the first found is returned.
    """
    select_columns = ['user_id', 'token', 'type', 'created_at']
    query_columns = []
    query_params = []
    if user_id:
        select_columns.remove('user_id')
        query_columns.append('user_id')
        query_params.append(user_id)
    if token:
        select_columns.remove('token')
        query_columns.append('token')
        query_params.append(token)
    if token_type:
        select_columns.remove('type')
        query_columns.append('type')
        query_params.append(token_type)
    if not query_columns:
        return None, ERR_INVALID_PARAMS
    query_columns = ' and '.join([column + ' = %s' for column in query_columns])
    if select_columns:
        select_str = ', '.join(select_columns)
    else:
        select_str = 'TRUE'

    query = "SELECT {} FROM {}.confirm_token WHERE {}".format(select_str, DB_SCHEMA, query_columns)
    rows, _ = execute_query_env(conn_id, query=query, params=query_params)
    if not rows:
        return None, ERR_CONFIRM_TOKEN_NOT_FOUND

    if single:
        return ConfirmToken(
            user_id=(user_id or rows[0][select_columns.index('user_id')]),
            token=(token or rows[0][select_columns.index('token')]),
            type=(token_type or rows[0][select_columns.index('type')]),
            created_at=rows[0][select_columns.index('created_at')]
        ), ERR_OK
    else:
        return [ConfirmToken(
            user_id=(user_id or row[select_columns.index('user_id')]),
            token=(token or row[select_columns.index('token')]),
            type=(token_type or row[select_columns.index('type')]),
            created_at=row[select_columns.index('created_at')]
        ) for row in rows], ERR_OK


def confirm_token_delete(conn_id, user_id=None, token=None, token_type=None, commit=True):
    """
    Removing tokens according to well -known data. Returns the number of remote tokens. 
    If not a single token was removed, a mistake.
    """
    query_columns = []
    query_params = []
    if user_id:
        query_columns.append('user_id')
        query_params.append(user_id)
    if token:
        query_columns.append('token')
        query_params.append(token)
    if token_type:
        query_columns.append('type')
        query_params.append(token_type)
    if not query_columns:
        return None, ERR_INVALID_PARAMS
    query_columns = ' and '.join([column + ' = %s' for column in query_columns])
    query = "DELETE FROM {}.confirm_token WHERE {} RETURNING TRUE".format(DB_SCHEMA, query_columns)
    rows, _ = execute_query_env(conn_id, query=query, params=query_params, commit=commit)
    if not rows:
        return 0, ERR_CONFIRM_TOKEN_NOT_FOUND

    return len(rows), ERR_OK


def create_confirm_email_token(conn_id, user_id, commit=True):
    return confirm_token_create(conn_id, user_id, token_type=TOKEN_EMAIL_CONFIRM, commit=commit)


def remove_confirm_email_tokens(conn_id, user_id=None, token=None, commit=True):
    return confirm_token_delete(conn_id, user_id=user_id, token=token, token_type=TOKEN_EMAIL_CONFIRM, commit=commit)


def get_confirm_email_token(conn_id, token, single=True):
    return confirm_token_get(conn_id, token=token, token_type=TOKEN_EMAIL_CONFIRM, single=single)


def create_password_reset_token(conn_id, user_id, commit=True):
    return confirm_token_create(conn_id, user_id, token_type=TOKEN_PASSWORD_RESET, commit=commit)


def remove_password_reset_tokens(conn_id, user_id=None, token=None, commit=True):
    return confirm_token_delete(conn_id, user_id=user_id, token=token, token_type=TOKEN_PASSWORD_RESET, commit=commit)


def get_password_reset_token(conn_id, token, single=True):
    return confirm_token_get(conn_id, token=token, token_type=TOKEN_PASSWORD_RESET, single=single)


def create_new_user_token(conn_id, user_id, commit=True):
    return confirm_token_create(conn_id, user_id, token_type=TOKEN_NEW_USER, commit=commit)


def remove_new_user_tokens(conn_id, user_id=None, token=None, commit=True):
    return confirm_token_delete(conn_id, user_id=user_id, token=token, token_type=TOKEN_NEW_USER, commit=commit)


def get_new_user_token(conn_id, token, single=True):
    return confirm_token_get(conn_id, token=token, token_type=TOKEN_NEW_USER, single=single)


def init_confirm_token(conn_id, commit=True):
    """
    Initialization of the table in the database
    :param conn_id:
    :param commit:
    """
    query = """
        CREATE TABLE IF NOT EXISTS {0}.confirm_token (
            user_id BIGINT,
            token VARCHAR(256),
            type CHAR(2),
            created_at TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS confirm_token_user_id_idx ON {0}.confirm_token (user_id);
        CREATE INDEX IF NOT EXISTS confirm_token_token_idx ON {0}.confirm_token (token);
    """.format(DB_SCHEMA)
    create_schema(conn_id, schema_name=DB_SCHEMA)
    execute_query_env(conn_id, query=query, fetch_result=False, commit=commit)


# Имя схемы
DB_SCHEMA = cfg_value('db_schema_confirm_token', default='service')