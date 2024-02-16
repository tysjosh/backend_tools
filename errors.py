ERR_OK = 0

ERR_INVALID_PARAMS = 201
ERR_DATABASE_DATA_CORRUPTION = 202

ERR_CUSTOM = 999

ERRORS = {
    ERR_OK: '',
    ERR_DATABASE_DATA_CORRUPTION: 'Database data corruption',
    ERR_INVALID_PARAMS: 'Invalid parameters'
}


def get_error_text(err_code) -> str:
    return ERRORS.get(err_code, 'Unknown error code ({})'.format(err_code))


def add_errors(*args):
    for index, src in enumerate(args):
        if ERRORS.keys() & src.keys():
            raise ValueError('Source #{} has duplicate error codes'.format(index))
        ERRORS.update(src)
