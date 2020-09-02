import functools
import inspect
from typing import Callable, Any

from .typing import RedMapping, KeyType, TTL, Encoder, Decoder


class CacheOpt:
    def __init__(self, mapping: RedMapping, key: KeyType):
        self._mapping = mapping
        self.key = (lambda *args, **kwargs: key) if isinstance(key, (str, bytes)) else key
        self._method = None

    @property
    def mapping(self) -> RedMapping:
        return self._mapping

    @property
    def method(self):
        return self._method

    def mount(self, method: Callable[..., Any]) -> 'CacheOpt':
        self._method = method
        return self

    def __call__(self, *args, **kwargs):
        pass


class CacheIt(CacheOpt):

    def __init__(self, mapping: RedMapping, key: KeyType, ttl: TTL = None, encoder: Encoder = None,
                 decoder: Decoder = None, force: bool = False):
        super().__init__(mapping, key)
        self._ttl = ttl
        self._encoder = encoder
        self._decoder = decoder
        self._force = force

    def __call__(self, *args, **kwargs):
        key = self.key(*args, **kwargs)
        if not self._force:
            cache = self.mapping.get(key)
            if cache:
                return self._decoder(cache)
        ret = self.method(*args, **kwargs)
        cache = self._encoder(ret)
        self.mapping.set(key, cache, ex=self._ttl)
        return ret


class RemoveIt(CacheOpt):

    def __init__(self, mapping: RedMapping, key: KeyType, by_return: bool = False):
        super().__init__(mapping, key)
        self._by_return = by_return

    @property
    def by_return(self) -> bool:
        return self._by_return

    def __call__(self, *args, **kwargs):
        ret = self.method(*args, **kwargs)
        k = self.key(*args, **kwargs) if not self._by_return else self.key(ret)
        self.mapping.delete(k)
        return ret


class GenRemoveIt(RemoveIt):
    def __call__(self, *args, **kwargs):
        for item in self.method(*args, **kwargs):
            if self.by_return:
                self.mapping.delete(self.key(item))
            yield item
        if not self.by_return:
            self.mapping.delete(self.key(*args, **kwargs))


class _RmOptFactory:
    @staticmethod
    def new(mapping: RedMapping, func: Callable[..., Any], key: KeyType, by_return: bool = False):
        wrapper = GenRemoveIt if inspect.isgeneratorfunction(func) else RemoveIt
        return functools.wraps(func)(wrapper(mapping, key, by_return).mount(func))
