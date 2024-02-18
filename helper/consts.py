from backend_tools.configuration import cfg_value
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent

SCHEMA_TEMP = cfg_value('db_temp_schema_name', default='temp')