# import base64
import hashlib
import json
import os
import re
import sys
import traceback
from datetime import datetime, timezone
from hashlib import sha256
from os.path import isdir
from time import time
from typing import Any, Union, List
from bcrypt import hashpw, gensalt, checkpw

from decimal import Decimal

RE_ALPHA_DIGIT_ = re.compile(r'[^A-Za-z0-9_]')
RE_ALPHA_DIGIT = re.compile(r'[^A-Za-z0-9]')
RE_ALPHA = re.compile(r'[^A-Za-z]')
RE_DIGIT = re.compile(r'[^0-9]')

RE_OBJECT_AT = re.compile(r'object\sat\s0x[0-9a-fA-F]+')


digits58 = '_123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz'
base_count_58 = len(digits58)
FILE_CHUNK_SIZE = 1024 * 1024 * 10


# Notifier colors
NC_OK = '#00FF00'
NC_WARN = '#EBB434'
NC_FATAL = '#FF0000'


def get_current_script_fullpath():
    return os.path.realpath(sys.argv[0])


def get_current_script_path():
    return os.path.dirname(os.path.realpath(sys.argv[0]))


def get_current_script_filename():
    return os.path.basename(sys.argv[0])


def set_work_path(path=None):
    os.chdir(path or get_current_script_path())


def proc_print(mess: str, timestamp: bool = True):
    stamp = '[{}] '.format(timestamp_to_str(tzignore=True)) if timestamp else ''
    proc_name = sys.argv[0].split('/')[-1]
    print('{}[{}] {}'.format(stamp, proc_name, mess))  # noqa: T001


def utctimestamp(dt: datetime) -> float:
    return dt.replace(tzinfo=timezone.utc).timestamp()


def timestamp_to_str(timestamp=None, fmt='%Y-%m-%d %H:%M:%S', tzignore=False):
    timestamp = timestamp if timestamp else time()
    if tzignore:
        return datetime.utcfromtimestamp(timestamp).strftime(fmt)
    else:
        return datetime.fromtimestamp(timestamp).strftime(fmt)


def str_to_timestamp(str_date, fmt='%Y-%m-%d'):
    return datetime.strptime(str_date, fmt).timestamp()


def elapsed_str(start, decimals=2, minutes=False) -> str:
    elapsed = time() - start
    suffix = ' sec'
    if minutes:
        elapsed /= 60
        suffix = ' min'

    tmpl = '{}{}{}'.format('{:.', decimals, 'f}{}')
    return tmpl.format(elapsed, suffix)


def estimated_str(start_time, total, progress, decimals=0, minutes=False, left=False) -> str:
    progress = progress if progress > 0 else 1
    elapsed = time() - start_time
    estimate = float(total) / progress * elapsed
    left_time = (float(total) / progress - 1) * elapsed
    left_time = left_time if left_time > 0 else 0

    suffix = ' sec'
    if minutes:
        estimate /= 60
        left_time /= 60
        suffix = ' min'

    result = left_time if left else estimate

    tmpl = '{}{}{}'.format('{:.', decimals, 'f}{}')
    return tmpl.format(result, suffix)


def to_serializable(src: Any) -> Any:
    if isinstance(src, Decimal):
        return float(src)
    elif isinstance(src, dict):
        return {key: to_serializable(value) for key, value in src.items()}
    elif isinstance(src, list):
        return [to_serializable(value) for value in src]
    elif isinstance(src, tuple):
        return tuple(to_serializable(value) for value in src)
    else:
        return src


def swap_dict(src):
    return dict(zip(src.values(), src.keys()))


def deep_update(obj: dict, update: dict):
    for k, v in update.items():
        if isinstance(obj.get(k), dict) and isinstance(v, dict):
            deep_update(obj[k], v)
            continue
        obj[k] = v


def get_text_hash(text):
    return sha256(text.encode()).hexdigest()


def to_csv_value(value):
    if value is None:
        value = ''
    elif isinstance(value, str):
        value = '"{}"'.format(str(value).replace('"', '""'))
    else:
        value = str(value)
    return value


def write_to_io(dst_io, *args):
    dst_io.write(';'.join([to_csv_value(arg) for arg in args]) + '\n')
    return


def load_data(filename):
    try:
        with open(filename, 'r') as f:
            data = f.read().split('\n')
        return data
    except Exception:
        return None


def save_data(filename, data):
    try:
        with open(filename, 'w') as f:
            f.write('\n'.join(data))
        return True
    except Exception:
        return False


def load_json(filename):
    try:
        with open(filename, 'r') as f:
            data = f.read()
        return json.loads(data), None
    except Exception as e:
        return None, '{}'.format(str(e))


def save_json(filename, data):
    try:
        with open(filename, 'w') as f:
            f.write(json.dumps(data))
        return True, None
    except Exception as e:
        return False, '{}'.format(str(e))


def check_path(src_path):
    if isdir(src_path):
        return

    try:
        os.makedirs(src_path)
        proc_print('\n== NEW folder created: {} == \n'.format(src_path))
    except FileExistsError:
        pass


def touched(file_path: str, last_update=None) -> Union[int, None]:
    actual_mtime = int(os.path.getmtime(file_path)) if os.path.isfile(file_path) else 0
    if not last_update or actual_mtime > last_update:
        return actual_mtime
    else:
        return None


def dict_change_key(obj, old_key, new_key):
    if old_key in obj:
        obj[new_key] = obj.pop(old_key)


def base58e(addr_int):
    if not addr_int:
        return ''
    _str = ''
    while addr_int >= base_count_58:
        (addr_int, mod) = divmod(addr_int, base_count_58)
        _str = digits58[mod] + _str
    if addr_int:
        _str = digits58[addr_int] + _str
    return _str


def base58d(addr):
    n = 0
    for char in addr:
        n = n * base_count_58 + digits58.index(char)
    return n

def get_salted_hash(password):
    return hashpw(get_text_hash(password).encode(), gensalt()).decode()


def check_salted_hash(password, pswd_hash):
    if pswd_hash is None:
        return False
    return checkpw(get_text_hash(password).encode(), pswd_hash.encode())


def add_checksum(hash_obj, location):
    with open(location, 'rb') as f:
        for chunk in iter(lambda: f.read(FILE_CHUNK_SIZE), b''):
            hash_obj.update(chunk)


def check_file_checksum(location, checksum):
    hash_obj = hashlib.sha256()
    add_checksum(hash_obj, location)
    return hash_obj.hexdigest() == checksum


class ProtectedPropertyObject(object):
    def __getattr__(self, item):
        try:
            return self.__dict__['_{}'.format(item)]
        except KeyError:
            try:
                return self.__class__.__dict__['_get__{}'.format(item)](self)
            except KeyError:
                raise ValueError('Unknown property: {}'.format(item))


def get_2datasource_bounds(offset: int, limit: int, first_length: int):
    delta = first_length if offset >= first_length else offset
    return offset - delta, limit + delta, delta, delta + limit


# ========================== DEPRECATION STUB ==========================
class DeprecationStub:
    def warn(self, *args, **kwargs):
        return

deprecation = DeprecationStub()
# ========================== WARNING STUB ==========================


class DatabaseStatementTimeout(Exception):
    def __init__(self, message: str = None, params=None):
        self.message = message
        self.params = params


def get_last_error_message() -> str:
    message = traceback.format_exc()
    n = message.rfind('\n', 0, -1)
    message = message[n + 1:] + message[:n]
    message = re.sub(RE_OBJECT_AT, 'object at 0x...', message)
    return message


def minimize_error_message(src: str, errors: List[str] = None) -> Union[str, None]:
    if not errors:
        return

    for entry in src.split('\n'):
        if entry and any(im in entry for im in errors or []):
            return entry

    return