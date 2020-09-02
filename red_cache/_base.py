import abc
import functools

from ._cache import CacheIt, _RmOptFactory
from .typing import RedMapping, KeyType, _DecoratorFunc, TTL, Encoder, Decoder, json_encoder, json_decoder, _Func


class _BaseMapping(RedMapping, metaclass=abc.ABCMeta):
    def cache_it(self, key: KeyType, ttl: TTL = None, encoder: Encoder = json_encoder, decoder: Decoder = json_decoder,
                 force: bool = False) -> _DecoratorFunc:
        def _warp(func: _Func):
            it = CacheIt(self, key, ttl, encoder, decoder, force)
            return functools.wraps(func)(it)

        return _warp

    def remove_it(self, key: KeyType, by_return: bool = False) -> _DecoratorFunc:
        def _wraps(func: _Func) -> _Func:
            it = _RmOptFactory.new(self, func, key, by_return)
            return functools.wraps(func)(it)

        return _wraps
