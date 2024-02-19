from backend_tools.sqlwerks import execute_query_env, create_schema as _create_schema

_DB_SCHEMA = 'service'


def get_service_schema_name():
    return _DB_SCHEMA


def create_schema(conn_id, schema_name=_DB_SCHEMA, read_only_user=None):
    """ Helper to create a service scheme """
    _create_schema(conn_id, schema_name=schema_name, read_only_user=read_only_user)


def _drop_schema(conn_id, schema_name=_DB_SCHEMA):
    execute_query_env(conn_id, query="DROP SCHEMA IF EXISTS {}".format(schema_name), fetch_result=False)
