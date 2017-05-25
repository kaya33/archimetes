import pyreBloom
import time
import datetime
import ast
import logging
import sys
from redis_base import Redis
from user_log import UserLog
from conf.config_default import configs

logging.basicConfig(level=logging.WARNING, format="%(asctime)s-%(name)s-%(levelname)s-%(message)s")
# maxmemory 4096mb
# maxmemory-policy allkeys-lru

class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class BloomFilter():

    # __metaclass__ = Singleton

    def __init__(self):

        self.capacity = configs.get('bloom_filter', {}).get('capacity', 3000)
        self.error_rate = configs.get('bloom_filter', {}).get('error_rate', 0.01)
        self.rebuild_time = configs.get('bloom_filter', {}).get('rebuild_time', 86400)
        self.redis_obj = UserLog()
        self.redis_base_obj = Redis()

    def filter_ad_by_user(self, user_id, ad_id_list, filter_key='rec_id'):
        try:
            p = pyreBloom.pyreBloom(user_id, self.capacity, self.error_rate)
            in_ele = set(p.contains([str(x[filter_key]) for x in ad_id_list]))
            return [x for x in ad_id_list if str(x[filter_key]) not in in_ele]
        except Exception as e:
            logging.error('[bloom filter]filter err, user_id:{0}, ad_id:{1}, err:{2}'.format(user_id, ad_id_list, e))
            return ad_id_list

    def save(self, user_id, ad_id_list, method='rec'):

        r = self.redis_base_obj.connect()
        time_now = time.time()

        if type(ad_id_list) != list:
            ad_id_list = [ad_id_list]
        p = pyreBloom.pyreBloom(user_id, self.capacity, self.error_rate)
        p.extend(ad_id_list)
        self.redis_obj.insert(user_id, ad_id_list, method)

        if r:
            try:
                time_update = r.get('bloom_filter_update' + user_id)
                if time_update and time_now - float(time_update) > self.rebuild_time:
                    self.build_from_redis(user_id)
                r.set('bloom_filter_update' + user_id, time_now)
            except Exception as e:
                logging.error('[bloom filter]save err, user_id:{0}, ad_id:{1}, err:{2}'.format(user_id, ad_id_list, e))
        else:
            logging.error('[bloom filter] redis obj is None')

    def build_from_redis(self, user_id):

        p = pyreBloom.pyreBloom(user_id, self.capacity, self.error_rate)
        p.delete()
        p.extend(self.redis_obj.select(user_id, num=500))


def test():
    pass
    # a = BloomFilter()
    # a.build_from_redis()
    # a.save_traffic('123', 'x')
    # a.save_traffic('123', ['y', 'm'])
    # print a.filter_ad_by_user('123', ['w', 'x', 'y', 'z'])


if __name__ == '__main__':
    pass

