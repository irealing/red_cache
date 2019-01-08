# red_cache

#### 介绍
基于Redis实现的Python缓存工具
#### 示例
##### 安装方法
```shell
$ pip install red-cache==0.0.2
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

@author:[Memory_Leak](http://vvia.xyz/wjLSh5)