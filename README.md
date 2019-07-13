# red_cache

#### 介绍
基于Redis实现的Python缓存工具
#### 示例
##### 安装方法
```shell
$ pip install red-cache
```

##### 缓存函数执行结果

```python
@redis_cache.pickle_cache(key=lambda v, t: "cache:{}:{}".format(v, t), ex=180)
def hell_world(val: str, times: int):
    return val * times
```
##### 保存JSON缓存数据

```python

@redis_cache.json_cache(key=lambda v, t: "cache:{}:{}".format(v, t), ex=180)
def hell_world(val: str, times: int):
    return {"val": val, "times": times}

```

##### 缓存的属性

```python
class Demo:
    def load_xxx(self):
        return

    xxx = redis_cache.property(key=lambda: "Demo::xxx", ex=10)(lambda self: self.load_xxx())

```
#### 删除缓存

```python

@redis_cache.remove(lambda o: "auth::user:{}".format(o))
def modify_user(user_id):
    # DO MODIFY USER
    pass

```
使用返回值

```python

@redis_cache.remove(lambda o: "auth::user:{}".format(o), by_return=True)
def modify_user(user_id):
    # DO MODIFY USER
    return "*********"
```
使用生成器

```python
@redis_cache.remove(lambda o: "auth::user:{}".format(o), by_return=True)
def modify_users(users):
    # modify users
    for u in users:
        yield u

```

#### 基于Redis的分布式锁

```python

@red_cache.red_lock(lambda uid, **kwargs: "red::lock:{}".format(uid), ttl=100000, retry_times=10, retry_delay=200)
def modify_user(uid: str, **kwargs):
    # DO MODIFY USER
    pass

```

#### 临时令牌工具

```python
import uuid

from red_cache import RedisCache, CachedToken

red_cache = RedisCache(dict(host='192.168.99.213', db=9))


# 声明 Token令牌类，集成CachedToken
class Token(CachedToken, metaclass=red_cache.token_meta):

    # 使用metaclass时可自动注入RedisCache对象到当前类对象
    # 使用类属性`red_cache`指定绑定的RedisCache亦可

    def __init__(self, token: str, username: str):
        super().__init__()
        self.token = token
        self.username = username

    # 使用cache_key_prefix指定缓存名称前缀
    cache_key_prefix = property(lambda self: self.__class__.__name__)

    # id 即当前令牌对象唯一值
    @property
    def id(self):
        return self.token

    # 返回字典，CachedToken使用标准库的json包序列化该字典作为对应缓存的值
    def marshal(self) -> dict:
        return dict(token=self.token, username=self.username)

    @classmethod
    def new(cls, username: str):
        return cls(token=uuid.uuid1().hex, username=username)


if __name__ == '__main__':
    tk = Token.new('/**/').save()
    # 使用ID读取令牌
    tk = Token.read(tk.token)
    # 刷新，即强制写入令牌到Redis
    tk.flush()
    # 删除令牌
    tk.remove()

```



@author:[Memory_Leak](https://github.com/irealing/red_cache)