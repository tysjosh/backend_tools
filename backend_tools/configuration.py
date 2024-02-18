
from typing import Dict
from configobj import ConfigObj
from misc import get_current_script_filename, touched

DEFAULT_SECTION = 'common'


class ConfigEnv:
    def __init__(self):
        self._cfg = ConfigObj()

        self._root_path = ''
        self._file_cfg_global = self._root_path + 'etc/backend.cfg'
        self._file_cfg_local = self._root_path + 'etc/backend-local.cfg'

        self._sec_environment = None

        self._timestamp_cfg_global, self._timestamp_cfg_local = self._get_cfgs_update_time()

        self.reload()

    def _get_cfgs_update_time(self, global_time=None, local_time=None):
        return touched(self._file_cfg_global, global_time), touched(self._file_cfg_local, local_time)

    def _cfg_value(self, value_name, section_name):
        section = self._cfg_section(section_name)
        return section.get(value_name) if section else None

    def _cfg_section(self, section_name):
        return self._cfg.get(section_name)

    def _init_service_props(self):
        self._sec_environment = self._cfg_value('ENVIRONMENT_SECTION', 'common') or DEFAULT_SECTION

    def reload(self):
        cfg = ConfigObj(self._file_cfg_global)
        cfg_local = ConfigObj(self._file_cfg_local)
        cfg.merge(cfg_local)
        del cfg_local

        self._cfg.clear()
        self._cfg.merge(cfg)
        del cfg

        self._init_service_props()

    def get_value(self, value_name, section_name, cast=None, default=None):
        value = (
            self._cfg_value(value_name, section_name) or
            self._cfg_value(value_name, self._sec_environment) or
            self._cfg_value(value_name, DEFAULT_SECTION)
        )
        if value is None:
            return default

        if cast and cast is bool:
            src = {'true': True, 'false': False}
            result = src.get(value.lower(), None)
            if result is None:
                raise ValueError('"{}" must be "true" or "false"'.format(value_name))
        else:
            result = cast(value) if cast else value
        return result

    def get_section_raw(self, section_name: str) -> Dict:
        section = self._cfg_section(section_name)
        return section.dict() if section else {}

    def is_cfg_updated(self) -> bool:
        t_global, t_local = self._get_cfgs_update_time(self._timestamp_cfg_global, self._timestamp_cfg_local)
        self._timestamp_cfg_global = t_global or self._timestamp_cfg_global
        self._timestamp_cfg_local = t_local or self._timestamp_cfg_local
        if t_global or t_local:
            return True
        return False

    def patch_cfg(self, patch: Dict):
        for section, content in patch.items():
            if section not in self._cfg:
                self._cfg[section] = {}
            for k, v in content.items():
                self._cfg[section][k] = v

        self._init_service_props()


def cfg_value(value_name, section_name=get_current_script_filename(), cast=None, default=None):
    return _cfg_obj.get_value(value_name, section_name, cast, default)


def is_cfg_updated() -> bool:
    return _cfg_obj.is_cfg_updated()


def cfg_reload() -> None:
    _cfg_obj.reload()


def cfg_section_raw(section_name: str) -> Dict:
    return _cfg_obj.get_section_raw(section_name)


class PatchCfg:
    def __init__(self, patch: Dict):
        self._patch = patch

    def __enter__(self):
        _cfg_obj.patch_cfg(self._patch)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        _cfg_obj.reload()


_cfg_obj = ConfigEnv()