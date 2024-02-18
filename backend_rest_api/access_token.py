from datetime import datetime
from time import time

from backend_tools.logger import debug
from backend_tools.misc import utctimestamp
from backend_tools.service.misc import create_schema

from backend_rest_api.errors import http_error, add_http_errors
from backend_rest_api.tools import generate_token
from backend_tools.configuration import cfg_value
from backend_tools.decorators import raise_controller_error
from backend_tools.errors import ERR_OK
from backend_tools.sqlwerks import execute_query_env

from backend_rest_api.orm import AbstractORMObject
from backend_rest_api.validators import CustomToken

TOKEN_LENGTH = 64

# коды 1 - 199 зарезервированы для авторизации
ERR_AUTHORIZATION_FAILED = http_error(100, 'Authorization failed', 401)
ERR_ACCESS_TOKEN_EXPIRED = http_error(101, 'Access token expired', 401)
ERR_REFRESH_TOKEN_EXPIRED = http_error(102, 'Refresh token expired', 401)
add_http_errors()


class Token(AbstractORMObject):
    def __init__(self, **kwargs):
        self._user_id = kwargs.pop('user_id', 0)
        self._refresh = kwargs.pop('refresh', '')
        self._refresh_expiration = kwargs.pop('refresh_expiration', 0)
        self._access = kwargs.pop('access', '')
        self._access_expiration = kwargs.pop('access_expiration', 0)

    def _get__data(self):
        return {
            'access_token': {'token': self.access, 'valid_to': self.access_expiration},
            'refresh_token': {'token': self.refresh, 'valid_to': self.refresh_expiration}
        }


def tokens_get(conn_id, access=None, refresh=None):
    """
    Возвращает Token по указанному access ИЛИ refresh
    :return: объект Token или None, если не найден
    """
    if not access and not refresh:
        return None

    query = """
        SELECT user_id, refresh, cast(extract(epoch from refresh_expiration) as integer),
            access, cast(extract(epoch from access_expiration) as integer)
        FROM {}.token
        WHERE {} = %s"""
    if access:
        query = query.format(DB_SCHEMA, 'access')
        token = access
    else:
        query = query.format(DB_SCHEMA, 'refresh')
        token = refresh
    row, _ = execute_query_env(conn_id, query=query, params=[token], single_row=True)
    return Token(
        user_id=row[0], refresh=row[1], refresh_expiration=row[2],
        access=row[3], access_expiration=row[4]) if row else None


def tokens_create(conn_id, user_id, commit=True):
    """
    Создает новый объект токена, сохраняет в базе
    :return: объект Token
    """
    refresh = generate_token(token_length=TOKEN_LENGTH)
    refresh_expiration = datetime.utcfromtimestamp(time() + TTL_REFRESH_TOKEN)
    access = generate_token(token_length=TOKEN_LENGTH)
    access_expiration = datetime.utcfromtimestamp(time() + TTL_ACCESS_TOKEN)

    query = "INSERT INTO {}.token (user_id, refresh, refresh_expiration, access, access_expiration) VALUES %s"
    execute_query_env(
        conn_id, query=query.format(DB_SCHEMA), fetch_result=False, commit=commit,
        params=[(user_id, refresh, refresh_expiration, access, access_expiration)])

    return Token(
        user_id=user_id, refresh=refresh, refresh_expiration=int(utctimestamp(refresh_expiration)),
        access=access, access_expiration=int(utctimestamp(access_expiration)))


def tokens_delete(conn_id, user_id=None, access=None, refresh=None, commit=True):
    """
    Удаление токена по любому из указанных параметров
    :param conn_id:
    :param user_id:
    :param access:
    :param refresh:
    :param commit:
    """
    if not user_id and not access and not refresh:
        return None

    if user_id:
        param = user_id
        field = 'user_id'
    elif access:
        param = access
        field = 'access'
    else:
        param = refresh
        field = 'refresh'

    query = "DELETE FROM {}.token WHERE {} = %s".format(DB_SCHEMA, field)
    execute_query_env(conn_id, query=query, params=[param], commit=commit, fetch_result=False)


@raise_controller_error
def check_access_token(conn_id, access):
    """
    Проверяет доступ по токену, возвращает user_id, если передан корректный токен.
    :param conn_id: 
    :param access: access_token
    :return: user_id ИЛИ Ошибка "Access expired"
    """
    query = "SELECT user_id, access_expiration FROM {}.token WHERE access = %s".format(DB_SCHEMA)
    row, _ = execute_query_env(conn_id, query=query, params=[access], single_row=True)
    if not row:
        return None, ERR_ACCESS_TOKEN_EXPIRED

    (user_id, access_expiration) = row
    if datetime.utcnow() < access_expiration:
        return user_id, ERR_OK

    return None, ERR_ACCESS_TOKEN_EXPIRED


@raise_controller_error
def refresh_tokens(conn_id, refresh, log_fn=debug):
    """
    Генерирует новую пару токенов, если передан корректный refresh.
    Прежний (или просроченный) токен удаляется.
    :param conn_id: 
    :param refresh: refresh_token
    :param log_fn:
    :return: Token ИЛИ Ошибка "Refresh expired"
    """
    query = "SELECT user_id, refresh_expiration FROM {}.token WHERE refresh = %s".format(DB_SCHEMA)
    row, _ = execute_query_env(conn_id, query=query, params=[refresh], single_row=True)
    if not row:
        log_fn(f"Refresh Tokens: {refresh} not found")
        return None, ERR_REFRESH_TOKEN_EXPIRED

    tokens_delete(conn_id, refresh=refresh)
    (user_id, access_expiration) = row
    if datetime.utcnow() < access_expiration:
        log_fn(f"Refresh Tokens: {refresh} updated for user={user_id}")
        return tokens_create(conn_id, user_id), ERR_OK

    log_fn(f"Refresh Tokens: {refresh} expired for user={user_id}")
    return None, ERR_REFRESH_TOKEN_EXPIRED


def clear_expired_tokens(conn_id):
    """
    Удаляет токены, у которых просрочена refresh_expiration
    """
    query = "DELETE FROM {}.token WHERE refresh_expiration < %s".format(DB_SCHEMA)
    execute_query_env(conn_id, query=query, params=[datetime.utcnow()])


def clear_all_tokens(conn_id, commit=True):
    """
    Очищает таблицу токенов
    """
    data, _ = execute_query_env(conn_id, query="DELETE FROM {}.token RETURNING 1".format(DB_SCHEMA), commit=commit)
    return len(data)


def get_access_token_user_map(conn_id):
    """
    Возвращает dict token -> user_id
    """
    q = "SELECT access, user_id FROM {0}.token"
    token_user, _ = execute_query_env(conn_id, query=q.format(DB_SCHEMA))
    return dict(token_user)


def remove_rabbidz(conn_id, allowed_count):
    """
    Удаляет свежие токены пользователей, если их количество привысило допустимое значение
    :param conn_id: 
    :param allowed_count: допустимое значение одновременных сессий пользователей
    :return: кол-во удаленных токенов
    """
    query = """
        WITH rabbidz as (
            SELECT access_expiration FROM {0}.token
            WHERE extract(epoch from access_expiration)::BIGINT > %s
            ORDER BY access_expiration OFFSET %s LIMIT 1
        )
        DELETE FROM {0}.token
        WHERE access_expiration >= (SELECT * FROM rabbidz)
        RETURNING access_expiration
    """.format(DB_SCHEMA)
    rabbidz, _ = execute_query_env(conn_id, query=query, params=[time(), allowed_count], commit=True)
    return len(rabbidz)


def init_access_token(conn_id, commit=True):
    """
    Инициализация таблицы mail_queue в БД
    :param conn_id: 
    :param commit: 
    """
    query = """
        CREATE TABLE IF NOT EXISTS {0}.token (
            user_id BIGINT,
            refresh CHAR(64),
            refresh_expiration TIMESTAMP,
            access CHAR(64),
            access_expiration TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS token_user_id_idx ON {0}.token (user_id);
        CREATE INDEX IF NOT EXISTS token_refresh_idx ON {0}.token (refresh);
        CREATE INDEX IF NOT EXISTS token_refresh_expiration_idx ON {0}.token (refresh_expiration);
        CREATE INDEX IF NOT EXISTS token_access_idx ON {0}.token (access);
    """.format(DB_SCHEMA)
    create_schema(conn_id, schema_name=DB_SCHEMA)
    execute_query_env(conn_id, query=query, fetch_result=False, commit=commit)


# Имя схемы
DB_SCHEMA = cfg_value('db_schema_access_token', default='service')

# Время жизни access_token
TTL_ACCESS_TOKEN = cfg_value('ttl_access_token', cast=int, default=3600)
# Время жизни refresh_token
TTL_REFRESH_TOKEN = cfg_value('ttl_refresh_token', cast=int, default=3600 * 24 * 60)


class RefreshToken(CustomToken):
    def __init__(self, **kwargs):
        super(RefreshToken, self).__init__(length=TOKEN_LENGTH, **kwargs)