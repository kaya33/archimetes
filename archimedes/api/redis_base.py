import redis
import json


# maxmemory 4096mb
# maxmemory-policy allkeys-lru

class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Redis():

    __metaclass__ = Singleton

    def __init__(self):

        conf = json.load(open('conf/redis_conf.json'))

        self.host = conf['host']
        self.port = conf['port']

        self.pool = redis.ConnectionPool(host=self.host, port=self.port)

    def connect(self):
        r = redis.StrictRedis(connection_pool=self.pool)
        return r


def test():
    r = Redis()
    # r.insert(123, 5123)
    # r.insert(123, 4123)
    # r.insert(123, 523)
    # r.insert(123, 513)
    # r.insert(123, 123)
    # print(r.select('123', dt='20170401'))

# test()
