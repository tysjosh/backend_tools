import json
import re
from datetime import datetime
import logging
from os import listdir, rmdir
from os.path import dirname, abspath, join, getsize, isfile, isdir
from shutil import make_archive, rmtree
from typing import Optional

from .misc import check_path, timestamp_to_str, elapsed_str, proc_print
from .configuration import cfg_value

__all__ = (
    'setup', 'debug', 'info', 'warning', 'error', 'fatal', 'to_log', 'total_time', 'destroy',
    'get_log_filepath', 'process_log_archivation',
    'LOG_DEBUG', 'LOG_INFO', 'LOG_WARNING', 'LOG_ERROR', 'LOG_FATAL', 'LOG_PATH', 'LOG_JSON',
    'GLOBAL_FOLDER_DATE_FORMAT'
)

LOG_DEBUG = logging.DEBUG
LOG_INFO = logging.INFO
LOG_WARNING = logging.WARNING
LOG_ERROR = logging.ERROR
LOG_FATAL = logging.FATAL

LOG_LEVEL_PREFIX = {
    LOG_DEBUG: '[DEBUG] ',
    LOG_INFO: '',
    LOG_WARNING: '[WARNING] ',
    LOG_ERROR: '[ERROR] ',
    LOG_FATAL: '[FATAL] '
}

DEFAULT_DATE_FMT = '%Y-%m-%d %H:%M:%S'
DEFAULT_FMT = '%Y-%m-%d_%H_%M_%S'

GLOBAL_FOLDER_DATE_FORMAT = '%Y-%m'
RE_GLOBAL_FOLDER_DATE_FORMAT = re.compile(r'^(\d{4}-\d{2})$')

DAILY_FMT = '%Y-%m-%d_#'
HOURLY_FMT = '%Y-%m-%d_%H_#'
MINUTELY_FMT = '%Y-%m-%d_%H_%M_#'

FILE_LIMIT_FMT = '%Y-%m-%d_#{:05d}'
RE_FILE_LIMIT_FMT = re.compile(r'(\d{4}-\d{2}-\d{2})_#(\d{5})')

LOG_PATH = cfg_value('LOG_PATH', default='')
LOG_LEVEL = cfg_value('LOG_LEVEL', cast=int, default=LOG_INFO)
LOG_CONSOLE_ONLY = cfg_value('LOG_CONSOLE_ONLY', cast=bool, default=False)
LOG_PREFIX = cfg_value('LOG_PREFIX', default='')
LOG_SUFFIX = cfg_value('LOG_SUFFIX', default='')
LOG_ERR_HANDLER = cfg_value('LOG_ERR_HANDLER', cast=bool, default=True)
LOG_FILENAME_FMT = cfg_value('LOG_FILENAME_FMT')
LOG_DAILY = cfg_value('LOG_DAILY', cast=bool, default=False)
LOG_HOURLY = cfg_value('LOG_HOURLY', cast=bool, default=False)
LOG_MINUTELY = cfg_value('LOG_MINUTELY', cast=bool, default=False)
LOG_FILE_LIMIT = cfg_value('LOG_FILE_LIMIT', cast=int, default=0) * 1024 * 1024
LOG_JSON = cfg_value('LOG_JSON', cast=bool, default=False)

_settings = {}


class ConsoleHandler(logging.Handler):
    def emit(self, record) -> None:
        proc_print(self.format(record))


class LoggerFilter(logging.Filter):
    def filter(self, record):
        record.label = (_settings.get('label', None) or (lambda: ''))()
        record.levelprefix = LOG_LEVEL_PREFIX[record.levelno]
        record.levellower = prepare_levelname(record.levelname)
        record.jsonmessage = json.dumps(record.msg)[1:-1]
        return True


def _get_logger_name(prefix, suffix):
    return '{}_{}'.format(prefix, suffix)


def _format_now(fmt=DAILY_FMT):
    return datetime.now().strftime(fmt)


def _get_log() -> logging.Logger:
    global _log
    if not _log:
        proc_print('[WARNING] Log has not been properly set up. Using console log by default')
        setup(console_only=True)
    return _log


def _get_logger_direct_path(log_path):
    prefix = _settings.get('prefix')
    suffix = _settings.get('suffix')
    filename_fmt = _settings.get('filename_fmt')
    err_handler = _settings.get('err_handler')
    console_only = _settings.get('console_only')
    file_limit = _settings.get('file_limit')
    _settings['log_open_date'] = _format_now()

    logger_name = _get_logger_name(prefix, suffix)
    log = logging.getLogger(logger_name)
    log.setLevel(_settings.get('log_level'))
    log.addFilter(LoggerFilter())

    if log_path is not None and not console_only:
        # File mode
        check_path(log_path)
        if _settings.get('log_json', LOG_JSON):
            formatter = logging.Formatter(json.dumps({
                'date': '%(asctime)s.%(msecs)03d',
                'log_level': '%(levellower)s',
                'message': '%(label)s%(jsonmessage)s',
            }), datefmt=DEFAULT_DATE_FMT)
        else:
            formatter = logging.Formatter('%(asctime)19s %(label)s%(levelprefix)s%(message)s', datefmt=DEFAULT_DATE_FMT)

        if file_limit:
            check_filename = _settings.get('current_file')
            # finds next index
            while True:
                filename_tmpl = '{}_{}'.format(_format_now(FILE_LIMIT_FMT), suffix)
                filename_tmpl = _generate_indexed_name(filename_tmpl, check_filename)
                check_filename = join(log_path, '{}.log'.format(filename_tmpl))
                if not isfile(check_filename) or getsize(check_filename) < _settings['file_limit']:
                    break
        else:
            filename_tmpl = '{}_{}'.format(_format_now(filename_fmt or DEFAULT_FMT), suffix)

        # Base handler
        log_file = '{}.log'.format(filename_tmpl)
        full_path = join(log_path, log_file)
        _settings['current_file'] = full_path
        handler = logging.FileHandler(full_path, encoding='UTF-8')
        handler.setFormatter(formatter)
        log.addHandler(handler)

        # Error/Fatal handler
        if err_handler:
            log_file_error = '{}_error.log'.format(filename_tmpl)
            handler_error = logging.FileHandler(join(log_path, log_file_error), encoding='UTF-8')
            handler_error.setFormatter(formatter)
            handler_error.setLevel(logging.ERROR)
            log.addHandler(handler_error)

        log.debug('Logger folder: {}'.format(log_path))
        log.debug('Logger file: {}'.format(log_file))
    else:
        # console mode
        formatter = logging.Formatter('%(label)s%(levelprefix)s%(message)s')
        handler = ConsoleHandler()
        handler.setFormatter(formatter)
        log.addHandler(handler)

    return log


def _reinit_log():
    global _log
    close_logger(_log)
    _setup(**_settings)


def _generate_indexed_name(template, src):
    index = 0
    if src:
        date, index = re.findall(RE_FILE_LIMIT_FMT, src)[0]
        date_t, _ = re.findall(RE_FILE_LIMIT_FMT, template.format(0))[0]
        index = int(index)
        if date == date_t:
            index += 1

    return template.format(index)


def _setup(**kwargs):
    global _log, _log_path_ext
    _settings.update(**kwargs)

    log_path = _settings.get('log_path')
    _log_path_ext = None
    if log_path is not None:
        _log_path_ext = join(log_path, _format_now(GLOBAL_FOLDER_DATE_FORMAT), _settings.get('prefix'))

    _log = _get_logger_direct_path(_log_path_ext)
    return _log


def setup(
        log_path=LOG_PATH, prefix=LOG_PREFIX, suffix=LOG_SUFFIX, log_level=LOG_LEVEL, err_handler=LOG_ERR_HANDLER,
        filename_fmt=LOG_FILENAME_FMT, label=None, daily=LOG_DAILY, hourly=LOG_HOURLY, minutely=LOG_MINUTELY,
        file_limit=LOG_FILE_LIMIT, log_json=LOG_JSON, console_only=LOG_CONSOLE_ONLY):
    destroy()
    return _setup(
        log_path=log_path, prefix=prefix, suffix=suffix, err_handler=err_handler, log_level=log_level,
        filename_fmt=filename_fmt, label=label, daily=daily, hourly=hourly, minutely=minutely, file_limit=file_limit,
        log_json=log_json, console_only=console_only)


def prepare_levelname(levelname):
    return {"critical": "fatal"}.get(levelname.lower(), levelname.lower())


def _reinit_required():
    if _settings.get('console_only', LOG_CONSOLE_ONLY):
        return False

    # Priority to file size limit
    if _settings.get('file_limit', None):
        # Workaround for multithreads - using stored filename instead object's filename
        try:
            fsize = getsize(_settings['current_file'])
        except FileNotFoundError:
            fsize = 0
        if fsize > _settings['file_limit']:
            return True
        elif _settings['log_open_date'] != _format_now():
            return True

    # Minutely filename mode
    elif _settings.get('minutely', None) and _settings['log_open_date'] != _format_now(MINUTELY_FMT):
        _settings['filename_fmt'] = MINUTELY_FMT
        return True
    # Hourly filename mode
    elif _settings.get('hourly', None) and _settings['log_open_date'] != _format_now(HOURLY_FMT):
        _settings['filename_fmt'] = HOURLY_FMT
        return True
    # Daily filename mode
    elif _settings.get('daily', None) and _settings['log_open_date'] != _format_now():
        _settings['filename_fmt'] = DAILY_FMT
        return True


def to_log(*args, level: int = LOG_INFO, console_only=False):
    message = ' '.join([str(x) for x in args])
    if console_only:
        proc_print(message)
        return

    if _reinit_required():
        _reinit_log()

    log = _get_log()
    method = {
        LOG_DEBUG: log.debug,
        LOG_INFO: log.info,
        LOG_WARNING: log.warning,
        LOG_ERROR: log.error,
        LOG_FATAL: log.fatal
    }[level]
    method(message)


def total_time(start_time):
    info('Total time: {}\n'.format(elapsed_str(start_time)))


def debug(*args):
    if _settings.get('log_level', LOG_LEVEL) <= LOG_DEBUG:
        to_log(*args, level=LOG_DEBUG)


def info(*args):
    if _settings.get('log_level', LOG_LEVEL) <= LOG_INFO:
        to_log(*args, level=LOG_INFO)


def warning(mess):
    if _settings.get('log_level', LOG_LEVEL) > LOG_WARNING:
        return

    global _last_warning, _last_warning_cnt
    if _last_warning != mess:
        info('Last warning duplicated {} times'.format(_last_warning_cnt)) if _last_warning_cnt > 2 else None
        to_log(mess, level=LOG_WARNING)
        _last_warning_cnt = 0

    _last_warning = mess
    _last_warning_cnt += 1


def error(mess):
    if _settings.get('log_level', LOG_LEVEL) > LOG_ERROR:
        return

    global _last_error, _last_error_cnt
    if _last_error != mess:
        info('Last error duplicated {} times'.format(_last_error_cnt)) if _last_error_cnt > 2 else None
        to_log(mess, level=LOG_ERROR)
        _last_error_cnt = 0

    _last_error = mess
    _last_error_cnt += 1


def fatal(mess):
    to_log(mess, level=LOG_FATAL)


def get_filename(logobj):
    handlers = logobj.handlers[:]
    return [handler.baseFilename for handler in handlers]


def get_filepath(logobj):
    return [dirname(abspath(x)) for x in get_filename(logobj)]


def close_logger(logobj):
    handlers = logobj.handlers[:]
    for handler in handlers:
        handler.close()
        logobj.removeHandler(handler)
    del logobj


def destroy():
    global _log, _last_warning, _last_warning_cnt, _last_error, _last_error_cnt
    if not _log:
        return
    close_logger(_log)
    _settings.clear()
    _last_warning = ''
    _last_warning_cnt = 0
    _last_error = ''
    _last_error_cnt = 0


def remove_log_folder():
    if _log_path_ext and isdir(_log_path_ext) and not listdir(_log_path_ext):
        rmdir(_log_path_ext)


def get_log_filepath():
    return _settings.get('current_file', 'N/A')


def process_log_archivation(output=info):
    """
    Процесс архивации логов за предыдущие месяцы
    :param output: 
    :return: 
    """
    root = _settings['log_path']
    output('Checking log folder ({}) ...'.format(root))
    available_log_folders = list(sorted(listdir(root)))
    excludes = [timestamp_to_str(fmt=GLOBAL_FOLDER_DATE_FORMAT)]

    patients = []
    for entry in available_log_folders:
        res = re.findall(RE_GLOBAL_FOLDER_DATE_FORMAT, entry)
        if not res or res[0] in excludes:
            continue

        patients.append(entry)

    output('Found {} folders would be archived{}'.format(len(patients), ': {}'.format(patients) if patients else '.'))
    if not patients:
        return

    for chosen in patients:
        prepared = join(root, chosen)
        output('    processing {}...'.format(prepared))
        res = make_archive(base_name=prepared, format='gztar', root_dir=prepared)
        if res:
            output('        -> {}'.format(res))
            rmtree(prepared)
        else:
            output('[ERROR] Failed to create archive')

    output('Done')


_log = None  # type: Optional[logging.Logger]
_log_path_ext = None  # type: Optional[str]
_last_warning = ''
_last_warning_cnt = 0
_last_error = ''
_last_error_cnt = 0