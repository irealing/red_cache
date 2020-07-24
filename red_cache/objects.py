from typing import AnyStr, Generator, Tuple

from redis import Redis

from red_cache.typing import TTL
from .typing import RedMapping

__author__ = 'Memory_Leak<irealing@163.com>'


class RedCache(RedMapping):

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


class RedHash(RedMapping):
    def __iter__(self):
        return self.redis.hscan_iter(self.resource)

    def has(self, key: AnyStr) -> bool:
        return self.redis.hexists(self.resource, key)

    def find(self, match: AnyStr = None) -> Generator[Tuple[bytes, bytes], None, None]:
        return self.redis.hscan_iter() if not match else filter(lambda v: v == match, self)

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
