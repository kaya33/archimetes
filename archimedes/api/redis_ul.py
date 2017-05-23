import redis_base
import time
import datetime
import logging


class RedisUl(redis_base.Redis):

    def insert(self, user_id, ad_id, method='rec'):

        if method not in ['rec', 'view']:
            logging.error('redis methon is invalid')
            return
        # two weeks
        if method == 'rec':
            expire_time = 1209600
        else:
            expire_time = 43200

        ts = time.time()
        tmp_list = []
        r = self.connect()

        if type(ad_id) != list:
            ad_id = [ad_id]

        for x in ad_id:
            tmp_list.append(ts)
            tmp_list.append(x)
#sorted
        r.zadd(method + user_id, *tmp_list)
        r.zadd(method + 'user_id', ts, user_id)
        r.expire(method + user_id, expire_time)

    def select(self, user_id, num=None, dt='', method='view'):
# 111
        r = self.connect()
        if dt == '' and num is not None:
            result = r.zrange(method + user_id, -num, -1)
        elif dt == '' and num is None:
            ts_min = float((datetime.date.today() - datetime.timedelta(14)).strftime("%s"))
            ts_max = time.time()
            result = r.zrangebyscore(method + user_id, ts_min, ts_max)
        else:
            ts_min = float(datetime.datetime.strptime(dt, '%Y%m%d').strftime("%s"))
            ts_max = time.time()
            result = r.zrangebyscore(method + user_id, ts_min, ts_max)

        return result

    def delete_user_list(self):

        r = self.connect()
        r.delete('user_id')
