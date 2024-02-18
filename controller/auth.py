from typing import Tuple, Optional
from backend_rest_api.access_token import tokens_delete, tokens_create, Token, refresh_tokens, check_access_token, \
    ERR_AUTHORIZATION_FAILED
from backend_rest_api.confirm_token import get_new_user_token, remove_new_user_tokens, get_password_reset_token, \
    remove_password_reset_tokens
from backend_tools.decorators import raise_controller_error
from backend_tools.errors import ERR_OK
from backend_tools.logger import warning, info
from backend_tools.misc import check_salted_hash
from backend_tools.sqlwerks import do_commit
from domain.auth import gen_newb_token, get_existing_confirm_token, gen_new_reset_token
from domain.user import prepare_user_fields
from helper.errors import ERR_PASSWORD_MISMATCH, ERR_WRONG_LOGIN_PARAMS, ERR_USER_BLOCKED, \
    ERR_USER_EMAIL_EXISTS

from datasource.user import update_user
from domain.mail import send_invite, send_forgot
from domain.user import user_add, approve_new_user, user_get_by
from secrets import compare_digest

from helper.errors import ERR_NO_ACTIVATION
from model.user import User


@raise_controller_error
def user_register(conn_id, name, email, password, country, activity, company, phone):
    if user_get_by(conn_id, 'email', email) or user_get_by(conn_id, 'email', email, activated=False):
        return None, ERR_USER_EMAIL_EXISTS
    user = user_add(conn_id, name, email, password, country, activity, company, phone)
    token = gen_newb_token(conn_id, user)
    send_invite(conn_id, user, token)
    do_commit(conn_id)
    return None, ERR_OK


@raise_controller_error
def user_activate(conn_id, token):
    # compare token
    token_obj, err_code = get_new_user_token(conn_id, token)
    if not token_obj:
        return None, err_code
    user_id = token_obj.user_id

    approve_new_user(conn_id, user_id)
    # delete current reset-token
    removed_nu, err_code = remove_new_user_tokens(conn_id, token=token_obj.token, commit=False)
    if not removed_nu:
        warning(f'[{err_code}] Could not remove NU token: {token_obj.token[:20]}...')
    do_commit(conn_id)
    return None, ERR_OK


@raise_controller_error
def user_login(conn_id, email: str, password: str) -> Tuple[Optional[Tuple[Token, User]], int]:
    user = user_get_by(conn_id, 'email', email, activated=False)
    if user:
        return None, ERR_NO_ACTIVATION
    user = user_get_by(conn_id, 'email', email)
    if not user or not check_salted_hash(password, user.hash):
        return None, ERR_WRONG_LOGIN_PARAMS
    if user.deleted:
        return None, ERR_USER_BLOCKED
    tokens_delete(conn_id, user_id=user.id)
    tokens = tokens_create(conn_id, user.id)
    info(f'Successful login: user={user.id}')
    return (tokens, user), ERR_OK


@raise_controller_error
def user_refresh(conn_id, refresh_token):
    tokens, _ = refresh_tokens(conn_id, refresh_token, log_fn=info)
    user = user_get_by(conn_id, 'id', tokens.user_id)
    if not user:
        return None, ERR_AUTHORIZATION_FAILED
    if user.deleted:
        return None, ERR_USER_BLOCKED
    return (tokens, user), ERR_OK


@raise_controller_error
def user_forgot_email(conn_id, email):
    user = user_get_by(conn_id, 'email', email, ['id', 'person_name', 'email'])
    if not user:
        # Shhhhhhhh
        warning(f'Forgot request to unknown email: {email}')
        return None, ERR_OK
    token = gen_new_reset_token(conn_id, user.id)
    send_forgot(conn_id, user, token)
    do_commit(conn_id)
    return None, ERR_OK


@raise_controller_error
def user_activation_email(conn_id, email):
    user = user_get_by(conn_id, 'email', email, ['id', 'person_name', 'email'], activated=False)
    if not user:
        # Shhhhhhhh
        warning(f'Forgot request to unknown email: {email}')
        return None, ERR_OK
    confirm_token = get_existing_confirm_token(conn_id, user.id)
    if not confirm_token:
        warning(f'User dont have nu token. uid={user.id}')
        confirm_token = gen_newb_token(conn_id, user)

    send_invite(conn_id, user, confirm_token)
    do_commit(conn_id)
    return None, ERR_OK


@raise_controller_error
def user_reset(conn_id, token, password, confirm):
    if not compare_digest(password, confirm):
        return None, ERR_PASSWORD_MISMATCH

    # compare tokens
    token_obj, err_code = get_password_reset_token(conn_id, token)
    if not token_obj:
        return None, err_code

    # change password
    fields, _ = prepare_user_fields(conn_id, token_obj.user_id, password=password)
    update_user(conn_id, uid=token_obj.user_id, **fields)

    # delete current reset-token
    removed_pr, err_code = remove_password_reset_tokens(conn_id, token=token_obj.token)
    if not removed_pr:
        warning(f'[{err_code}] Could not remove used PR token: {token_obj.token[:20]}...')
    return None, ERR_OK


@raise_controller_error
def user_logout(conn_id, user: User):
    check_access_token(conn_id, user.access_token)
    tokens_delete(conn_id, access=user.access_token)
    return None, ERR_OK