import redis
import time
import datetime


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
        self.host = 'localhost'
        self.port = 6379
        self.pool = redis.ConnectionPool(host=self.host, port=self.port)

    def connect(self):
        r = redis.StrictRedis(connection_pool=self.pool)
        return r

    def insert(self, user_id, ad_id):

        ts = time.time()

        #two weeks
        expire_time = 1209600
        r = self.connect()
        r.zadd(user_id, ts, ad_id)
        r.expire(user_id, expire_time)

    def select(self, user_id, num=0, dt=''):

        r = self.connect()
        if dt == '':
            result = r.zrange(user_id, -num, -1, withscores=True)
        else:
            ts_min = float(datetime.datetime.strptime(dt, '%Y%m%d').strftime("%s"))
            ts_max = time.time()
            result = r.zrangebyscore(user_id, ts_min, ts_max, withscores=True)

        return result




def test():
    r = Redis()
    # r.insert(123, 5123)
    # r.insert(123, 4123)
    # r.insert(123, 523)
    # r.insert(123, 513)
    # r.insert(123, 123)
    print(r.select('123', dt='20170401'))

# test()
