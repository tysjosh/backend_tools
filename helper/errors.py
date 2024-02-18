from backend_rest_api.errors import http_error, add_http_errors


# [1000 - 1199] authorization + authentication
ERR_USER_EMAIL_EXISTS = http_error(1002, 'User with this email already exists', 400)
ERR_WRONG_PASSWORD = http_error(1003, 'Wrong password', 400)
ERR_PASSWORD_MISMATCH = http_error(1004, 'Entered passwords doesn\'t match', 400)
ERR_USER_CREATION = http_error(1005, 'Error creating new user', 404)
ERR_EMAIL_MISSING = http_error(1006, 'Required email parameter', 400)
ERR_WRONG_LOGIN_PARAMS = http_error(1011, 'Incorrect e-mail or password', 400)
ERR_USER_BLOCKED = http_error(1012, 'Account is blocked', 403)
ERR_TOO_MANY_LOGIN_ATTEMPTS = http_error(1013, 'Too many login attempts', 400)
ERR_NO_ACTIVATION = http_error(8001, 'Your email requires verification. Check your mailbox to activate it.', 400)


add_http_errors()


class LastProcessedException(Exception):
    def __init__(self, msg='Last processes table error', status_code=500):
        self.msg = msg
        self.status_code = status_code