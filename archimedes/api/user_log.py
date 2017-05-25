import redis_base
import time
import datetime
import logging

logging.basicConfig(level=logging.WARNING, format="%(asctime)s-%(name)s-%(levelname)s-%(message)s")


class UserLog(redis_base.Redis):

    def insert(self, user_id, ad_id, method='rec'):

        if method not in ['rec', 'view']:
            logging.error('[user_log]redis method is invalid')
            return []
        # two weeks and 30 days
        if method == 'rec':
            expire_time = 1209600
        else:
            expire_time = 2592000

        ts = time.time()
        tmp_list = []
        r = self.connect()
        if r is None:
            logging.error('[user_log]insert err, redis connect error')
            return []

        if type(ad_id) != list:
            ad_id = [ad_id]

        for x in ad_id:
            tmp_list.append(ts)
            tmp_list.append(x)

        try:
            r.zadd(method + user_id, *tmp_list)
            r.expire(method + user_id, expire_time)
            return ad_id
        except Exception as e:
            logging.error('[user_log]insert err, user_id:{0}, ad_id:{1}, err:{2}'.format(user_id, ad_id, e))
            return []

    def select(self, user_id, num=None, days=14, method='view'):

        r = self.connect()
        if r is None:
            logging.error('[user_log]select err, redis connect error')
            return []

        if num is not None:
            result = r.zrange(method + user_id, -num, -1)
        else:
            ts_min = float((datetime.date.today() - datetime.timedelta(days)).strftime("%s"))
            ts_max = time.time()
            result = r.zrangebyscore(method + user_id, ts_min, ts_max)

        return result
