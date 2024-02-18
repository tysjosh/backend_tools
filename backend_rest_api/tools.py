
import re
from base64 import b64encode, b32encode, b16encode
from random import randint
from time import time
from math import log


RE_ABSTRACT_TOKEN = re.compile(r'^[a-zA-Z0-9_-]+$')
DEF_TOKEN_LENGTH = 16


def generate_token(token_length=DEF_TOKEN_LENGTH, base=16, reverse=False, altchars=b'-_'):
    """
    Создает confirm token по принципу:
        source = "<текущее время X байт>" + "<рандомный набор Y байт>" => base64
    :param token_length: длина токена, x <= 16
    :param base: формат токена, один из [16, 32, 64]
    :param reverse: флаг выполнения .reverse() для токена 
    :param altchars: доп. символы для base64
    :return: сгенерированный token
    """
    base = base if base in [16, 32, 64] else 16
    token_length = 16 if token_length < 16 else token_length

    bits = int(log(base, 2))
    div, mod = divmod(bits * token_length, 8)
    ext_len = div - 4 + (1 if mod else 0)

    token_bytes = round(time()).to_bytes(4, byteorder='big')
    for i in range(ext_len):
        token_bytes += randint(1, 255).to_bytes(1, byteorder='big')

    if base == 64:
        result = b64encode(token_bytes, altchars=altchars).decode()
    elif base == 32:
        result = b32encode(token_bytes).decode()
    else:  # base == 16
        result = b16encode(token_bytes).decode()

    result = result[:token_length]
    return result[::-1] if reverse else result


def is_token_valid(token, length=DEF_TOKEN_LENGTH):
    """
    Проверка token на соответствие допустимому набору символов
    """
    return isinstance(token, str) and len(token) == length and re.match(RE_ABSTRACT_TOKEN, token) is not None
