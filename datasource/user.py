from typing import Iterable, Optional, Tuple, List, Dict

from backend_rest_api.confirm_token import init_confirm_token
from backend_tools.sqlbase import filter_data, delete_data
from backend_tools.sqlwerks import create_schema, execute_query_env, insert_many
from backend_tools.tabular import Tabular, query_iterator
from backend_tools.errors import ERR_OK
from backend_tools.builder.where import where

from helper.sql import build_select_expr, build_update_query
from helper.errors import ERR_USER_EMAIL_EXISTS
from model.user import User