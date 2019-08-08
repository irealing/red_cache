import json
import logging
import random
import time
import uuid
from abc import ABCMeta, abstractmethod
from functools import wraps
from typing import Any, Callable, Iterable, Generator, Optional
from typing import TypeVar, Type

from redis import Redis
from redis.exceptions import ConnectionError, TimeoutError

from .counter import RedCounter, HashCounter

try:
    import _pickle as pickle
except ImportError:
    import pickle
"""缓存工具"""

__author__ = 'Memory_Leak<irealing@163.com>'

T = TypeVar('T')


class IllegalException(Exception):
    def __init__(self, *args):
        super(Exception, self).__init__(*args)


class RedisCache(object):
    default_encoding = 'utf-8'

    def __init__(self, cfg):
        self._redis = Redis(**cfg)
        self.logger = logging.getLogger(self.__class__.__name__)

    redis = property(lambda self: self._redis)

    @staticmethod
    def pickle_encoder(obj: Any) -> bytes:
        return pickle.dumps(obj)

    @staticmethod
    def pickle_decoder(ret: bytes) -> Any:
        return pickle.loads(ret)

    @classmethod
    def json_encoder(cls, obj: Any) -> bytes:
        return json.dumps(obj)

    @classmethod
    def json_decoder(cls, ret: bytes) -> Any:
        return json.loads(ret, encoding=cls.default_encoding)

    def cache(self, key: Any, encoder: Callable[[Any], bytes], decoder: Callable[[bytes], Any], ex: Any = None):
        def _wraps(func):
            @wraps(func)
            def _do(*args, **kwargs):
                ck = key(*args, **kwargs) if callable(key) else key
                data = self._redis.get(ck)
                if data:
                    self.logger.debug("cache hit %s", key)
                    return decoder(data)
                ret = func(*args, **kwargs)
                data = encoder(ret)
                self.logger.debug("cache set %s", key)
                self._redis.set(ck, data, ex=ex)
                return ret

            return _do

        return _wraps

    def force_cache(self, key: Any, encoder: Callable[[Any], bytes] = pickle_encoder, ex: Any = None):
        def _wraps(func):
            @wraps(func)
            def _do(*args, **kwargs):
                ck = key(*args, **kwargs) if callable(key) else key
                ret = func(*args, **kwargs)
                data = encoder(ret)
                self.logger.debug("force update cache %s", ck)
                self._redis.set(ck, data, ex=ex)
                return ret

            return _do

        return _wraps

    def json_cache(self, key: Any, ex: Any = None):
        return self.cache(key, self.json_encoder, self.json_decoder, ex)

    def pickle_cache(self, key: Any, ex: Any = None):
        return self.cache(key, self.pickle_encoder, self.pickle_decoder, ex)

    def remove(self, key: Any, by_return: bool = False):
        def _wraps(func):
            @wraps(func)
            def _do(*args, **kwargs):
                ret = func(*args, **kwargs)
                if by_return:
                    keys = map(key, ret) if isinstance(ret, Generator) else (key(ret),)
                else:
                    _k = key(*args, **kwargs) if callable(key) else key
                    keys = (_k,)
                self._remove(keys)

            return _do

        return _wraps

    @staticmethod
    def _split_iterable(keys: Iterable[str], size: int = 10):
        buf = []
        for key in keys:
            buf.append(key)
            if len(key) >= size:
                yield buf
                buf.clear()
        if buf:
            yield buf

    def _remove(self, keys: Iterable[str], size: int = 10):
        """删除"""
        for names in self._split_iterable(keys, size):
            self._redis.delete(*names)

    def property(self, key: Any, ex: Any = None) -> Callable[[Callable], 'RedProperty']:
        def _wraps(func):
            ck = key() if callable(key) else key
            return RedProperty(self, ck, func, ex)

        return _wraps

    def red_lock(self, resource: Any, ttl: int = 100000, retry_times: int = 3, retry_delay: int = 200):
        def _wraps(func):
            @wraps(func)
            def __do(*args, **kwargs):
                res = resource(*args, **kwargs) if callable(resource) else resource
                with RedLock(self.redis, res, ttl, retry_times, retry_delay):
                    return func(*args, **kwargs)

            return __do

        return _wraps

    def counter(self, resource: str, step: int = 1, init: Optional[Callable[[], int]] = None) -> RedCounter:
        return RedCounter(self._redis, resource, step, init)

    def hash_counter(self, resource: str, key: str, step: int = 1, init: Optional[Callable[[], int]] = None):
        return HashCounter(self._redis, resource, key, step, init)

    def token_meta(self):
        def _new(cls_name, bases, attr):
            if CachedToken not in bases:
                raise IllegalException('class {} must be subclass of {}'.format(cls_name, CachedToken))
            attr.update(red_cache=self)
            return type(cls_name, bases, attr)

        return _new


class RedProperty(object):
    """从Redis加载的属性"""

    def __init__(self, redis_cache: RedisCache, key: str, method: Callable[[], Any], ex: Any = None):
        self._redis_cache = redis_cache
        self._method = method
        self._read_method = redis_cache.pickle_cache(key)(self._method)
        self._ex = ex
        self._key = key

    ex = property(lambda self: self._ex)
    key = property(lambda self: self._key)

    def __get__(self, instance, owner):
        if not instance:
            return self
        self._redis_cache.logger.debug("read redProperty %s", self.key)
        return self._read_method(instance)


class RedLockError(Exception):
    pass


class RedLock(object):
    DEFAULT_RETRY_TIMES = 3
    DEFAULT_RETRY_DELAY = 1000
    DEFAULT_TTL = 100000
    RELEASE_LUA_SCRIPT = """
    if redis.call("GET",KEYS[1]) == ARGV[1] then
        return redis.call("DEL",KEYS[1])
    else
        return 0
    end
    """
    logger = logging.getLogger("RedLock")

    def __init__(self, redis: Redis, resource: str, ttl: int = DEFAULT_TTL,
                 retry: int = DEFAULT_RETRY_TIMES, delay: int = DEFAULT_RETRY_DELAY):
        self._redis = redis
        # self._release_method = self._redis.register_script(self.RELEASE_LUA_SCRIPT)
        self._resource = resource
        self._retry = retry
        self._ttl = ttl
        self._delay = delay
        self._key_value = uuid.uuid1().hex

    def acquire(self):
        """获取锁"""
        self.logger.debug("acquire redLock %s %s", self._resource, self._key_value)
        ttl = int(time.time() * 1000) + self._ttl
        for retry in range(self._retry + 1):
            if self._acquire():
                return self
            if (time.time() * 1000) > ttl:
                raise RedLockError()
            delay = random.randint(0, self._delay) / 1000
            self.logger.debug("redLock acquire %s %s delay %s", self._resource, self._key_value, delay)
            time.sleep(delay)
        raise RedLockError()

    def _acquire(self):
        try:
            ret = self._redis.set(self._resource, self._key_value, nx=True, px=self._ttl)
            self.logger.debug("lock ret %s", ret)
            return ret
        except (ConnectionError, TimeoutError):
            return 0

    def release(self):
        """释放锁"""
        self.logger.debug("release redLock %s %s", self._resource, self._key_value)
        try:
            return self._redis.eval(self.RELEASE_LUA_SCRIPT, 1, self._resource, self._key_value)
            # return self._release_method(keys=[self._resource], args=[self._key_value])
        except (ConnectionError, TimeoutError):
            return 0

    def __enter__(self):
        return self.acquire()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.release()
        if exc_val:
            raise exc_val

    def locked(self):
        return self._redis.get(self._resource) == self._key_value


class CachedToken(metaclass=ABCMeta):
    red_cache: Optional[RedisCache]
    cache_key_prefix = ""
    cache_ttl = 1800

    @abstractmethod
    def __init__(self):
        pass

    @property
    @abstractmethod
    def id(self):
        pass

    @abstractmethod
    def marshal(self) -> dict:
        pass

    key = property(lambda self: "{}:{}".format(self.cache_key_prefix or self.__class__.__name__, self.id))

    def _save(self):
        self.red_cache.force_cache(self.key, encoder=self.red_cache.json_encoder, ex=self.cache_ttl)(self.marshal)()

    @classmethod
    def read(cls: Type[T], tid: str, flush: bool = True) -> Optional[T]:
        key = "{}:{}".format(cls.cache_key_prefix or cls.__name__, tid)
        ret = cls.red_cache.json_cache(key, ex=cls.cache_ttl)(lambda: None)()
        if not ret:
            return None
        return cls(**ret).flush() if flush else cls(**ret)

    def remove(self):
        self.red_cache.remove(self.key)(lambda: None)()

    def flush(self):
        self._save()
        return self

    def save(self):
        return self.flush()
