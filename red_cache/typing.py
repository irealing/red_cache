import abc
import json
import logging
import pickle
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


def json_encoder(o: Any) -> AnyStr:
    return json.dumps(o)


def pickle_encoder(o: Any) -> AnyStr:
    return pickle.dumps(o)


def json_decoder(data: bytes) -> Any:
    return json.loads(data)


def pickle_decoder(data: bytes) -> Any:
    return pickle.loads(data)


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

    def delete(self, key: str, *args: str) -> int:
        pass

    def counter(self, resource: AnyStr, step: int = 1, init: Union[Callable[[], int], int] = None) -> 'Counter':
        return Counter(self, resource, step, init)


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


class Counter(object):
    def __init__(self, mapping: RedMapping, resource: AnyStr, step: int = 1,
                 init: Union[Callable[[], int], int] = None):
        self.mapping = mapping
        self.resource = resource
        self._step = step
        if init:
            self.mapping.set(self.resource, init() if callable(init) else init)

    @property
    def value(self):
        return int(self.mapping.get(self.resource, '0'))

    def get(self, step: int = None) -> int:
        return int(self.mapping.incr(self.resource, step or self._step))

    def __get__(self, instance, owner):
        if not instance:
            return self
        return self.get()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.mapping.delete(self.resource)
        if exc_val:
            raise exc_val
