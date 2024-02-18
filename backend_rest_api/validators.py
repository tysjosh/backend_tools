import re
import trafaret as t
from datetime import datetime
from typing import Any

from backend_rest_api.tools import DEF_TOKEN_LENGTH, is_token_valid


RE_EMAIL = re.compile(r"""^(?:[a-z0-9!#$%&'*+/=?^_`{|}~-]+(?:\.[a-z0-9!#$%&'*+/=?^_`{|}~-]+)*|"(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21\x23-\x5b\x5d-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])*")@(?:(?:[a-z0-9](?:[a-z0-9-]*[a-z0-9])?\.)+[a-z0-9](?:[a-z0-9-]*[a-z0-9])?|\[(?:(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9]))\.){3}(?:(2(5[0-5]|[0-4][0-9])|1[0-9][0-9]|[1-9]?[0-9])|[a-z0-9-]*[a-z0-9]:(?:[\x01-\x08\x0b\x0c\x0e-\x1f\x21-\x5a\x53-\x7f]|\\[\x01-\x09\x0b\x0c\x0e-\x7f])+)\])$""")  # noqa: E501
RE_HASH = re.compile(r"(^[0-9a-fA-F]{64}$)")
RE_ALPHA_DIGIT_MINUS_ = re.compile(r'[^A-Za-z0-9_-]')
RE_ANY_HEX = re.compile(r"(^[0-9a-f]{1,}$)")


def check(validator: t.Trafaret, value: Any, raise_error: bool = False) -> bool:
    """
    Validation wrapper to catch error and return result of validation
    """
    try:
        validator.check(value)
    except t.DataError as e:
        if raise_error:
            raise e
        return False
    return True


class HexString(t.String):
    def check_value(self, value):
        match = re.match(RE_ANY_HEX, value)
        if not match or match.group() != value:
            self._failure(f'{value} must be a hex-string', value=value)


class Hash(t.String):
    def check_value(self, value):
        match = re.match(RE_HASH, value)
        if not match or match.group() != value:
            self._failure(f'{value} must contain exactly 64 symbols (digits and letters a-f, A-F)')


class CustomToken(t.String):
    def __init__(self, length=DEF_TOKEN_LENGTH, **kwargs):
        super(CustomToken, self).__init__(**kwargs)
        self.length = length

    def check_and_return(self, value):
        value = super(CustomToken, self).check_and_return(value)
        if not is_token_valid(value, length=self.length):
            self._failure("not valid token", value=value)

        return value


class CustomEnum(t.Trafaret):
    """
    Enum с автоматической пре-конвертацией входного значения.
    Если Enum содержит список однотипных значений - делается попытка конвертировать
        входящий параметр в этот тип перед валидацией

    Поддерживаемые типы: int, float
    """
    __slots__ = ['variants']

    def __init__(self, *variants):
        self.pre_convertor = type(variants[0])
        for entry in variants[1:]:
            if type(entry) != self.pre_convertor:
                self.pre_convertor = None
                break

        if self.pre_convertor not in (int, float):
            self.pre_convertor = None

        self.variants = variants[:]

    def check_and_return(self, value):
        if self.pre_convertor:
            try:
                value = self.pre_convertor(value)
            except Exception:
                pass

        if value not in self.variants:
            self._failure(
                "'{value}' must be one of a list of values: {variants}"
                .format(value=value, variants=self.variants),
                value=value
            )
        return value

    def __repr__(self):
        return "<CustomEnum(%s)>" % (", ".join(map(repr, self.variants)))


class CustomString(t.String):
    """
    t.String с опцией предвраительного вызова str.strip()
    """
    def __init__(self, allow_blank=False, min_length=None, max_length=None, pre_strip=None):
        super().__init__(allow_blank=allow_blank, min_length=min_length, max_length=max_length)
        self.pre_strip = bool(pre_strip)

    def check_and_return(self, value):
        if not isinstance(value, t.str_types):
            self._failure("value is not a string", value=value)
        if self.pre_strip:
            value = value.strip()
        return super().check_and_return(value)


class StripString(CustomString):
    def __init__(self, *args, **kwargs):
        kwargs['pre_strip'] = True
        super().__init__(*args, **kwargs)


class Date(t.String):
    """
    t.String представляющий из себя дату (YYYY-MM-DD)
    Конвертирует ее в datetime.date
    """
    def __init__(self, fmt='%Y-%m-%d'):
        super().__init__(allow_blank=False)
        self.fmt = fmt

    def check_and_return(self, value):
        value = super().check_and_return(value)
        try:
            return datetime.strptime(value, self.fmt).date()
        except ValueError:
            self._failure("'{}' does not match format '{}'".format(value, self.fmt))


IntDate = t.Int(gte=0, lte=((1 << 37) - 1))