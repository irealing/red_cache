from typing import Callable, Optional, Any

__author__ = 'Memory_Leak<irealing@163.com>'
__all__ = ('LazyProperty',)


class LazyProperty(object):
    def __init__(self, getter: Callable, setter: Optional[Callable] = None):
        self._getter = getter
        self._setter = setter
        self._value: Optional[Any] = None

    def __get__(self, instance, owner):
        if not instance:
            return self
        if not self._value:
            self._value = self._getter(instance)
        return self._value

    def __set__(self, instance, value):
        if not instance:
            return value
        if self._setter:
            self._value = self._setter(instance, value)
        raise AttributeError("can't set attribute")
