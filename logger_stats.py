import re
from datetime import datetime, timedelta
from genericpath import isdir
from os import listdir
from os.path import join

from configuration import cfg_value
from logger import LOG_PATH, warning, error

RE_TIME_DEFAULT = r'([0-9\.]{1,50})'
RE_TOTAL_DEFAULT = 'Total time: @TIME@'
RE_FATAL = re.compile('(\[FATAL\])|("log_level": "fatal")')  # noqa: W605
RE_ERROR = re.compile('\[ERROR\]|("log_level": "error")')  # noqa: W605
RE_WARNING = re.compile('\[WARNING\]|("log_level": "warning")')  # noqa: W605

EXCLUDE_DIRS = cfg_value('report_exclude_dirs', section_name='dev', default=[])

RES_KEY_TEMPLATE = '{:36}'
RES_VAL_TEMPLATE = '{:8} | {:14.2f} | {:9.2f} | {:8} | {:8} | {:8}'
RES_VAL_HEADER_TEMPLATE = '{:8} | {:14} | {:9} | {:8} | {:8} | {:8}'

KEY_TOTAL = '[TOTAL]'
KEY_MASK = '[MASK]'
KEY_ERROR = '[ERROR]'

DAILY_FILE = 0
MONTHLY_FILE = 1


def _get_source_filenames(path, log_day):
    if not isdir(path):
        return []

    dirfiles = listdir(path)
    result = [
        # files with requested date
        (filename, DAILY_FILE) for filename in dirfiles
        if filename[:len(log_day)] == log_day and filename[-4:] == '.log' and filename[-10:-4] != '_error']

    if len(log_day) > 7:
        log_month = log_day[:7] + '_'
        result += [
            # files with month of requested date
            (filename, MONTHLY_FILE) for filename in dirfiles
            if filename[:8] == log_month and filename[-4:] == '.log' and filename[-10:-4] != '_error']

    return result


def _get_part_from_monthly(text, log_day):
    if not text:  # text empty
        return ''

    if len(log_day) < 9:  # this happens when log_day is like '2017-12-'
        return text

    log_month = log_day[:7]

    first_line = text[:text.find('\n')]
    idx = 0
    while not first_line:  # if first line is empty
        idx += 1
        first_line = text[:text.find('\n', idx)]

    start = None
    if first_line[0] == '{':
        # json  : ^*"date": "2017-12-01
        log_pattern = '"date": "' + log_day
        log_json = True
    elif first_line[:7] == log_month:
        # usual : ^2017-12-01
        log_pattern = '\n' + log_day
        log_json = False
        if first_line[:len(log_day)] == log_day:
            start = 0
    else:
        error('Invalid file format, first line: "{}"'.format(first_line))
        return ''

    if start is None:
        start = text.find(log_pattern)
        if start == -1:
            # no match :(
            return ''

        if log_json:
            start = text.rfind('\n', None, start)
        start = 0 if start == -1 else start  # start of requested part

    end = text.rfind(log_pattern, start)
    end = text.find('\n', end + 1)  # end of requested part
    if end != -1 and not log_json:
        # try to find line with next date (last message can be multiline)
        end = text.find(log_pattern[:8], end)
    return text[start:end]


def _regexp_compile(log_format: str):
    return re.compile(log_format.replace('@TIME@', RE_TIME_DEFAULT))  # noqa: W605


def _log_report_core(path, log_format, log_day, custom_masklist):

    sources = _get_source_filenames(path, log_day)
    if not sources:
        return (*((0, ) * 6), {custom_mask: 0 for custom_mask in custom_masklist})

    average, time_sum, count, fatal_count, error_count, warning_count = (0, ) * 6
    custom_mask_counts = {custom_mask: 0 for custom_mask in custom_masklist}
    p = _regexp_compile(log_format)

    was_err = False
    for filename, file_type in sources:
        text = ''
        with open(join(path, filename), 'r', encoding='utf-8') as f:
            try:
                text = f.read()
            except Exception as e:
                warning('{}: {} (file {})'.format(e.__class__.__name__, str(e), join(path, filename)))
        if file_type == MONTHLY_FILE:
            text = _get_part_from_monthly(text, log_day)

        times = p.findall(text)
        for _time in times:
            count += 1
            try:
                time_sum += float(_time)
            except ValueError:
                was_err = True

        fatals = RE_FATAL.findall(text)
        fatal_count += len(fatals)
        errors = RE_ERROR.findall(text)
        error_count += len(errors)
        warnings = RE_WARNING.findall(text)
        warning_count += len(warnings)

        for custom_mask, mask_re in custom_masklist.items():
            if isinstance(mask_re, str):
                mask_re = re.compile(mask_re)
            matches = mask_re.findall(text)
            custom_mask_counts[custom_mask] += len(matches)

    warning('Invalid template format: "{}"'.format(log_format)) if was_err else None
    average = (time_sum / count) if count else 0
    return count, time_sum, average, fatal_count, error_count, warning_count, custom_mask_counts


def _create_report_entry(count, total, average, fatal_count, error_count, warning_count):
    return {
        'count': count,
        'total worktime': total,
        'average': average,
        'fatals': fatal_count,
        'errors': error_count,
        'warnings': warning_count,
    }


def _get_log_report_data(log_day, log_formats=None, log_path=LOG_PATH, custom_masklist=None):
    if log_day is None or log_day == 'yesterday':
        dir_month = str((datetime.utcnow() - timedelta(1)).date())[:7]  # yesterday
        log_day = str((datetime.utcnow() - timedelta(1)).date())
    elif log_day == 'today':
        dir_month = str(datetime.utcnow().date())[:7]  # today
        log_day = str(datetime.utcnow().date())
    else:
        dir_month = log_day[:7]

    log_formats = log_formats or {}
    result = {}
    all_count, all_worktime, all_fatals, all_errors, all_warnings = (0, ) * 5

    dir_prefix = ''
    root_folder = join(log_path, dir_month)
    if not isdir(root_folder):
        result[KEY_ERROR] = 'Oops, something went wrong! Seems like you entered wrong date...'
        return result

    if custom_masklist is None:
        custom_masklist = {}
    all_custom_mask_counts = {}

    # folders
    for dir_name in listdir(root_folder):
        if dir_name in EXCLUDE_DIRS:
            continue

        current_dir_prefix = dir_name.split('_')[0]
        if current_dir_prefix != dir_prefix and current_dir_prefix != dir_name:
            dir_prefix = current_dir_prefix
        log_format = log_formats.get(dir_name, RE_TOTAL_DEFAULT)

        count, total, average, fatal_count, error_count, warning_count, custom_mask_counts = _log_report_core(
            join(root_folder, dir_name), log_format, log_day, custom_masklist)

        if count or fatal_count or error_count or warning_count:
            result[dir_name] = _create_report_entry(count, total, average, fatal_count, error_count, warning_count)
            all_count += count
            all_worktime += total
            all_fatals += fatal_count
            all_errors += error_count
            all_warnings += warning_count

        for custom_mask, mask_count in custom_mask_counts.items():
            if not mask_count:
                continue
            if custom_mask not in all_custom_mask_counts:
                all_custom_mask_counts[custom_mask] = {}
            if dir_name not in all_custom_mask_counts[custom_mask]:
                all_custom_mask_counts[custom_mask][dir_name] = mask_count
            else:
                all_custom_mask_counts[custom_mask][dir_name] += mask_count

    # totals
    result[KEY_TOTAL] = _create_report_entry(all_count, all_worktime, 0, all_fatals, all_errors, all_warnings)

    # custom masks
    for custom_mask in all_custom_mask_counts:
        mask_result = {dir_name: count for dir_name, count in all_custom_mask_counts[custom_mask].items()}
        result[KEY_MASK + custom_mask] = mask_result

    return result


def get_log_report_formatted(log_day, log_formats=None, log_path=LOG_PATH, custom_masklist=None):
    data = _get_log_report_data(log_day, log_formats, log_path, custom_masklist)
    if KEY_ERROR in data:
        return data[KEY_ERROR]

    result = {RES_KEY_TEMPLATE.format(' '): (
        RES_VAL_HEADER_TEMPLATE.format('count', 'total worktime', 'average', 'fatals', 'errors', 'warnings'))}

    dir_prefix = ''
    for dir_name, metrics in data.items():
        if dir_name == KEY_TOTAL:
            continue

        if KEY_MASK in dir_name:
            custom_mask = dir_name.replace(KEY_MASK, '')
            result[RES_KEY_TEMPLATE.format('~~ ' + custom_mask)] = metrics
            continue

        current_dir_prefix = dir_name.split('_')[0]
        if current_dir_prefix != dir_prefix and current_dir_prefix != dir_name:
            dir_prefix = current_dir_prefix
            result[RES_KEY_TEMPLATE.format(dir_prefix + '_*')] = (
                RES_VAL_HEADER_TEMPLATE.format('-' * 8, '-' * 14, '-' * 9, '-' * 8, '-' * 8, '-' * 8))

        result[RES_KEY_TEMPLATE.format(dir_name if len(dir_name) <= 36 else (dir_name[:33] + '...'))] = (
            RES_VAL_TEMPLATE.format(
                metrics['count'], metrics['total worktime'], metrics['average'],
                metrics['fatals'], metrics['errors'], metrics['warnings']))

    result[RES_KEY_TEMPLATE.format('~')] = (
        RES_VAL_HEADER_TEMPLATE.format('-' * 8, '-' * 14, '-' * 9, '-' * 8, '-' * 8, '-' * 8))
    totals = data[KEY_TOTAL]
    result[RES_KEY_TEMPLATE.format('~ Totals')] = (
        RES_VAL_TEMPLATE.format(
            totals['count'], totals['total worktime'], totals['average'],
            totals['fatals'], totals['errors'], totals['warnings']))

    return result


def get_log_report_json(log_day, log_formats=None, log_path=LOG_PATH, custom_masklist=None):
    return _get_log_report_data(log_day, log_formats, log_path, custom_masklist)


def get_log_report(log_day, log_formats=None, log_path=LOG_PATH, custom_masklist=None):
    result = get_log_report_formatted(log_day, log_formats, log_path, custom_masklist)
    result[RES_KEY_TEMPLATE.format('        DEPRECATED')] = 'Use `get_log_report_formatted` instead of this'
    return result