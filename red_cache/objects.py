from typing import AnyStr, Generator, Tuple

from redis import Redis

from ._base import _BaseMapping
from .typing import TTL

__author__ = 'Memory_Leak<irealing@163.com>'


class RedCache(_BaseMapping):

    def __init__(self, redis: Redis):
        super().__init__(redis, "")

    @classmethod
    def from_url(cls, url: str) -> 'RedCache':
        return cls(Redis.from_url(url))

    def __iter__(self):
        return self.redis.scan_iter()

    def has(self, key: AnyStr) -> bool:
        return self.redis.exists()

    def find(self, match: AnyStr = None) -> Generator[Tuple[bytes, bytes], None, None]:
        return self.redis.scan_iter() if not match else filter(lambda value: value == match, self)

    def set(self, key: AnyStr, value: AnyStr, ex: TTL = None):
        return self.redis.set(key, value, ex=ex)

    def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        return self.redis.get(key) or default_value

    def incr(self, key: AnyStr, value: int = 1) -> int:
        return self.redis.incrby(key, value)

    def size(self) -> int:
        return self.redis.dbsize()

    def clear(self) -> int:
        return self.redis.flushdb()

    def exists(self) -> bool:
        return True

    def delete(self, key: str, *args: str) -> int:
        return self.redis.delete(key, *args)

    def red_hash(self, resource: AnyStr) -> 'RedHash':
        return RedHash(self.redis, resource)


class RedHash(_BaseMapping):
    def __iter__(self):
        return self.redis.hscan_iter(self.resource)

    def has(self, key: AnyStr) -> bool:
        return self.redis.hexists(self.resource, key)

    def find(self, match: AnyStr = None) -> Generator[Tuple[bytes, bytes], None, None]:
        return self.redis.hscan_iter(self.resource) if not match else filter(lambda v: v == match, self)

    def set(self, key: AnyStr, value: AnyStr, ex: TTL = None):
        return self.redis.hset(self.resource, key, value)

    def get(self, key: AnyStr, default_value: AnyStr = None) -> bytes:
        value = self.redis.get(key)
        return value if value is None else default_value

    def incr(self, key: AnyStr, value: int = 1) -> int:
        return self.redis.hincrby(self.resource, key, value)

    def clear(self) -> int:
        return self.redis.delete(self.resource)

    def size(self) -> int:
        return self.redis.hlen(self.resource)

    def exists(self) -> bool:
        return self.redis.exists(self.resource)

    def delete(self, key: str, *args: str) -> int:
        return self.redis.hdel(self.resource, key, *args)
