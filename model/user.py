from dataclasses import dataclass
from typing import List
from backend_rest_api.model import OldModel
from backend_tools.errors import ERR_OK
from backend_tools.misc import check_salted_hash
from helper.hashid import hashid



@dataclass
class User(OldModel):
    id: int = None
    email: str = None
    hash: str = None
    person_name: str = None
    apikey: str = None
    role: str = None
    country: str = None
    phone: str = None
    activity: str = None
    company: str = None

    deleted: bool = None
    created: int = None
    last_login: int = None
    last_login_attempt: int = None
    activated_at: int = None
    access_token: str = None  # todo::optimize Move to special access object or delete



    @classmethod
    def sql_map(cls, alias=None):
        alias = f'{alias}.' if alias else ''
        return {
            'created': f"extract(epoch from {alias}created)::BIGINT as created",
            'last_login': f"extract(epoch from {alias}last_login)::BIGINT as last_login",
            'last_login_attempt': f"extract(epoch from {alias}last_login_attempt)::BIGINT as last_login_attempt",
            'activated_at': f"extract(epoch from {alias}activated_at)::BIGINT as activated_at"
        }

    def is_password_match(self, password):
        return check_salted_hash(password, self.hash)

    def is_role_match(self, allowed_roles):
        return self.role in allowed_roles

    @property
    def safe_person_name(self):
        return self.person_name or ''

    @property
    def names_map(self):
        return {
            'person_name': 'name'
        }

    @property
    def include_null(self):
        return ['activated_at']

    @property
    def excluded_fields(self):
        return [
            'hash', 'access_token', 'last_login', 'last_login_attempt',
            'settings', 'unlimited', 'permissions', 'permitted_users', 'enriched'
        ]

    def as_dict(self, *fields, force_null: List[str] = None, should_hash_id=True):
        user = super().as_dict(*fields, force_null=force_null)
        if should_hash_id and 'id' in fields:
            user['id'] = hashid(user['id'])
        return user