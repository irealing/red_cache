import abc
import functools
from typing import Callable, Any

from ._cache import CacheOpt, CacheIt, _RmOptFactory
from .typing import RedMapping, KeyType, _DecoratorFunc, TTL, Encoder, Decoder, json_encoder, json_decoder, _Func
from .typing import pickle_encoder, pickle_decoder

__author__ = 'Memory_Leak<irealing@163.com>'


class _BaseMapping(RedMapping, metaclass=abc.ABCMeta):
    @staticmethod
    def _wrapped(opt: CacheOpt, func: Callable[..., Any]):
        opt.mount(func)

        @functools.wraps(func)
        def _(*args, **kwargs):
            return opt(*args, **kwargs)

        return _

    def cache_it(self, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder, decoder: Decoder = json_decoder,
                 force: bool = False) -> _DecoratorFunc:
        def _warp(func: _Func):
            it = CacheIt(self, key, ttl, encoder, decoder, force)
            return self._wrapped(it, func)

        return _warp

    def remove_it(self, key: KeyType, by_return: bool = False) -> _DecoratorFunc:
        def _wraps(func: _Func) -> _Func:
            it = _RmOptFactory.new(self, func, key, by_return)
            return self._wrapped(it, func)

        return _wraps

    def json_cache(self, key: KeyType, ex: TTL = None):
        return self.cache_it(key, ttl=ex)

    def pickle_cache(self, key: KeyType, ex: TTL = None):
        return self.cache_it(key, ttl=ex, encoder=pickle_encoder, decoder=pickle_decoder)

    def remove(self, key: KeyType, by_return: bool = False):
        return self.remove_it(key, by_return=by_return)
