import functools
from time import time

from crontab import CronTab
from errors import ERR_CUSTOM, get_error_text
from misc import elapsed_str


class timeit(object):
    def __init__(self, stdout=None, prefix=''):
        self.stdout = stdout if stdout else self._print
        self.prefix = prefix

    def __call__(self, func):
        @functools.wraps(func)
        def core(*args, **kwargs):
            start = time()
            result = func(*args, **kwargs)
            self.stdout('{}{}: {}'.format(self.prefix, func.__name__, elapsed_str(start)))
            return result

        return core

    @staticmethod
    def _print(mess):
        """Cython workaround - can't be used builtin 'print'"""
        print(mess)  # noqa: T001


class ControllerError(Exception):
    def __init__(self, **kwargs):
        self.code = kwargs.pop('code', ERR_CUSTOM)
        self.message = kwargs.pop('message', 'Details not found')
        self.data = kwargs.pop('data', None)
        self.ext_metadata = kwargs.pop('ext_metadata', None)

    def __str__(self):
        return '[{}]: {}'.format(self.code, self.message)


def raise_controller_error(func=None, custom_error_decoder=None):

    def get_decorated(fn):
        def core(*args, raise_error=True, error_decoder=None, with_data=False, **kwargs):
            result, err_code = fn(*args, **kwargs)
            if raise_error and err_code:
                error_message = error_decoder(err_code) if callable(error_decoder) else get_error_text(err_code)
                raise ControllerError(code=err_code, message=error_message, data=result if with_data else None)
            else:
                return result, err_code

        return core

    def decorator(fn):
        return functools.partial(get_decorated(fn), error_decoder=custom_error_decoder)

    return decorator if custom_error_decoder else get_decorated(func)


class CronTaskGenerator(object):

    def __init__(self, cron):
        self.cron = CronTab(cron)
        self.next_start = time() + self.cron.next()

    def __call__(self, func):
        @functools.wraps(func)
        def core(*args):
            yield

            while True:
                if not self.time_has_come:
                    yield
                    continue

                self.next_start = time() + self.cron.next()
                func(*args)
                yield

        return core

    @property
    def time_has_come(self):
        return self.next_start <= time()