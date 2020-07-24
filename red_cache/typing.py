import abc
import logging
from datetime import timedelta
from typing import AnyStr, Generator, Tuple, Union, Callable, TypeVar, Any

from redis import Redis

_Ret = TypeVar('_Ret')
TTL = Union[int, timedelta]
_Func = Callable[..., _Ret]
_DecoratorFunc = Callable[[_Func], _Func]
Encoder = Callable[[Any], AnyStr]
Decoder = Callable[[bytes], Any]
KeyType = Union[AnyStr, Callable[[], AnyStr]]


class RedObject(metaclass=abc.ABCMeta):
    def __init__(self, redis: Redis, resource: AnyStr):
        self._resource = resource
        self._redis = redis
        self.logger = logging.getLogger(self.__class__.__name__)

    @property
    def resource(self):
        return self._resource

    @property
    def redis(self) -> Redis:
        return self._redis

    @abc.abstractmethod
    def clear(self) -> int:
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.clear()

    @abc.abstractmethod
    def size(self) -> int:
        pass

    @abc.abstractmethod
    def exists(self) -> bool:
        pass


class RedMapping(RedObject, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def __iter__(self):
        pass

    @abc.abstractmethod
    def has(self, key: AnyStr) -> bool:
        pass

    @abc.abstractmethod
    def find(self, match: AnyStr = None) -> Generator[Tuple[bytes, bytes], None, None]:
        pass

    @abc.abstractmethod
    def set(self, key: AnyStr, value: AnyStr, ex: TTL = None):
        pass

    @abc.abstractmethod
    def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        pass

    @abc.abstractmethod
    def incr(self, key: AnyStr, value: int = 1) -> int:
        pass


class RedCollection(RedObject, metaclass=abc.ABCMeta):
    @abc.abstractmethod
    def add(self, value: AnyStr, *args: AnyStr) -> int:
        pass

    @abc.abstractmethod
    def remove(self, value: AnyStr) -> int:
        pass

    @abc.abstractmethod
    def __iter__(self) -> Generator[bytes, None, None]:
        pass

    def filter(self, f: Callable[[bytes], bool] = lambda _: True) -> Generator[bytes, None, None]:
        yield from filter(f, self)
