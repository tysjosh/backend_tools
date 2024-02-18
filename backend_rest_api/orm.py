"""
Поля объекта должны начинаться с "_".

Формат для функции-getter'а: _get__<property_name>

Пример:
    class MyObj(AbstractORMObject):
        def __init__(self, **kwargs):
            self._id = kwargs.pop('id', '')
            self._data = kwargs.pop('data', '')

        def _get__status(self):
            return 'status'

    obj = MyObj(id=1, data='some data')
    print(obj.id, obj.data, obj.status)
"""
from backend_tools.misc import ProtectedPropertyObject


class AbstractORMObject(ProtectedPropertyObject):
    pass