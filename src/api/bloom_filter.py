import pyreBloom
import time
import datetime

from redis_me import Redis


# maxmemory 4096mb
# maxmemory-policy allkeys-lru

class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Bf():

    __metaclass__ = Singleton

    def __init__(self):

        self.capacity = 500
        self.error_rate = 0.01
        self.redis_obj = Redis()

    def filter_ad_by_user(self, user_id, ad_id_list):

        p = pyreBloom.pyreBloom(user_id, self.capacity, self.error_rate)
        in_ele = set(p.contains(ad_id_list))
        return [x for x in ad_id_list if x not in in_ele]

    def save_traffic(self, user_id, ad_id_list):

        if type(ad_id_list) != list:
            ad_id_list = [ad_id_list]
        p = pyreBloom.pyreBloom(user_id, self.capacity, self.error_rate)
        p.extend(ad_id_list)
        self.redis_obj.insert(user_id, ad_id_list)


def test():
    a = Bf()
    a.save_traffic('123', 'x')
    a.save_traffic('123', ['y', 'm'])
    print a.filter_ad_by_user('123', ['w', 'x', 'y', 'z'])

# test()