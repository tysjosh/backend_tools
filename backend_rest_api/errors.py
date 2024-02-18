from backend_tools.errors import ERR_OK, ERR_INVALID_PARAMS, ERR_DATABASE_DATA_CORRUPTION, add_errors

# [400 - 599] service API error codes
ERR_VALIDATE_PARAMS = 400

ERRORS = {}
HTTP_STATUS_CODES = {
    ERR_OK: 200,
    ERR_INVALID_PARAMS: 400,
    ERR_DATABASE_DATA_CORRUPTION: 500,
    ERR_VALIDATE_PARAMS: 400
}


def http_error(err_code, err_message, status_code):
    if err_code in HTTP_STATUS_CODES:
        raise ValueError('Error code #{} already exists'.format(err_code))
    ERRORS[err_code] = err_message
    HTTP_STATUS_CODES[err_code] = status_code
    return err_code


def get_status_code(err_code):
    """
    Returns http error code by given internal error code

    **Params:**

        :param err_code: internal error code
        :return: http error code
    """
    return HTTP_STATUS_CODES.get(err_code, 404)


def add_http_errors():
    """ Registers newly added http errors. Clears the buffer after registering """
    add_errors(ERRORS)
    ERRORS.clear()


ERR_USER_NOT_FOUND = http_error(1, 'User not found', 404)
ERR_PERMISSION_DENIED = http_error(149, 'Permission denied', 403)
ERR_OPERATION_TOO_LONG = http_error(410, 'Operation too long - timeout. Try again later', 504)
add_http_errors()