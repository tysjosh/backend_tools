from backend_rest_api.confirm_token import remove_password_reset_tokens, create_password_reset_token, \
    remove_new_user_tokens, create_new_user_token, confirm_token_get, TOKEN_NEW_USER
from backend_tools.logger import info


def gen_new_reset_token(conn_id, user_id):
    # Удаляем существующие reset-token'ы для юзера
    removed, _ = remove_password_reset_tokens(conn_id, user_id, commit=False)
    info('Removed {} old PR tokens (user #{})'.format(removed, user_id)) if removed else None

    token, _ = create_password_reset_token(conn_id, user_id=user_id, commit=True)
    return token


def gen_newb_token(conn_id, user):
    # Удаляем существующие nu-token'ы для юзера
    removed, _ = remove_new_user_tokens(conn_id, user.id, commit=False)
    info('Removed {} old NU tokens (user #{})'.format(removed, user.id)) if removed else None

    token, _ = create_new_user_token(conn_id, user_id=user.id, commit=False)
    return token


def get_existing_confirm_token(conn_id, user_id):
    token, _ = confirm_token_get(conn_id, user_id=user_id, token_type=TOKEN_NEW_USER, single=True)
    return token