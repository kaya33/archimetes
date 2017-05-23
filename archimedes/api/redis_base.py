import redis
import json
import logging

# maxmemory 4096mb
# maxmemory-policy allkeys-lru
logging.basicConfig(level=logging.WARNING, format="%(asctime)s-%(name)s-%(levelname)s-%(message)s")


class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Redis():

    __metaclass__ = Singleton

    def __init__(self):

        conf = json.load(open('/mnt/sdb/archimetes/archimedes/api/conf/redis_conf.json'))

        self.host = conf['host']
        self.port = conf['port']
        try:
            self.pool = redis.ConnectionPool(host=self.host, port=self.port)
        except Exception as e:
            logging.error('redis pool init err:{}'.format(e))

    def connect(self):

        if self.pool:
            r = redis.StrictRedis(connection_pool=self.pool)
            return r
        else:
            try:
                self.pool = redis.ConnectionPool(host=self.host, port=self.port)
                r = redis.StrictRedis(connection_pool=self.pool)
                return r
            except Exception as e:
                logging.error('redis pool init err:{}'.format(e))
                return None


def test():
    r = Redis()
    # r.insert(123, 5123)
    # r.insert(123, 4123)
    # r.insert(123, 523)
    # r.insert(123, 513)
    # r.insert(123, 123)
    # print(r.select('123', dt='20170401'))

# test()
