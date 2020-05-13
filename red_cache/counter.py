"""基於Redis的計數器工具"""
from abc import ABCMeta, abstractmethod
from typing import Callable, Optional

from redis import Redis

__author__ = 'Memory_Leak<irealing@163.com>'


class Counter(metaclass=ABCMeta):
    def __init__(self, redis: Redis, resource: str, step: int):
        self._redis = redis
        self._step = step
        self._resource = resource

    @property
    def redis(self):
        return self._redis

    step = property(lambda self: self._step)
    resource = property(lambda self: self._resource)

    @abstractmethod
    def get(self):
        raise NotImplementedError()


class RedCounter(Counter):
    _init_script = """
    if (redis.call("exists", KEYS[1]) ~= 1 or KEYS[4] == "true") then
        if (tonumber(KEYS[3]) > 0) then
            redis.call("setex", KEYS[1], KEYS[3], KEYS[2])
        else
            redis.call("set", KEYS[1], KEYS[2])
        end
    end
    """

    def __init__(self, redis: Redis, resource: str, step: int, init: Optional[Callable[[], int]] = None,
                 ex: int = 0, force_init: bool = False):
        super().__init__(redis, resource, step)
        self._amount = abs(step)
        self._getter = self.decr if step < 0 else self.incr
        if init:
            self._setup(resource, init(), ex, force_init)

    def _setup(self, resource: str, value: int, ex: int, force: bool):
        self.redis.eval(self._init_script, 4, resource, value, ex, 'true' if force else 'false')

    value = property(lambda self: int(self._redis.get(self._resource)) or 0)

    def get(self):
        return self._getter()

    def incr(self):
        return self._redis.incrby(self._resource, self._amount)

    def decr(self):
        return self._redis.decrby(self._resource, self._amount)

    def __get__(self, instance, owner):
        if not instance:
            return self
        return self.value


class HashCounter(Counter):
    def __init__(self, redis: Redis, resource: str, key: str, step: int, init: Optional[Callable[[], int]] = None):
        super().__init__(redis, resource, step)
        self._key = key
        if init:
            redis.hset(resource, key, init())

    key = property(lambda self: self._key)
    value = property(lambda self: int(self._redis.hget(self.resource, self._key)) or 0)

    def get(self):
        return self._redis.hincrby(self.resource, self.key, self.step)

    def __get__(self, instance, owner):
        if not instance:
            return self
        return self.get()
