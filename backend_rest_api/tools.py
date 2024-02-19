
import re
from base64 import b64encode, b32encode, b16encode
from random import randint
from time import time
from math import log


RE_ABSTRACT_TOKEN = re.compile(r'^[a-zA-Z0-9_-]+$')
DEF_TOKEN_LENGTH = 16


def generate_token(token_length=DEF_TOKEN_LENGTH, base=16, reverse=False, altchars=b'-_'):
    """
    Creates confirm token according to the principle: 
        Source = "<The current time x byte>" + "<random set y byt>" => basy64 
    : param token_length: token length, x <= 16 
    : Param Base: Token format, one of [16, 32, 64] 
    : Param Reverse: Flag Fulfillment .reverse () for token 
    : Param altchars: additional. Symbols for Base64 
    : return: generated token
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
    Token check for compliance with the permissible set of characters
    """
    return isinstance(token, str) and len(token) == length and re.match(RE_ABSTRACT_TOKEN, token) is not None
