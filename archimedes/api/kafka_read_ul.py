#-*- coding: utf-8 -*-
import json
import threading
import requests
import logging
import time
import jieba.analyse
import math
from bloom_filter import Bf
from mongo_base import Mongo
from kafka import KafkaConsumer


class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(
                Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class KafkaUlConsumer():

    # __metaclass__ = Singleton

    def __init__(self):

        conf = json.load(
            open('/mnt/sdb/archimetes/archimedes/api/conf/kafka_conf.json'))
        self.consumer = []

        self.host = conf['host']
        self.group_id = conf['group_id']
        self.timeout = conf['timeout']
        self.topic = conf['topic']
        self.consume_num = conf['consume_num']
        self.mysql_api = conf['mysql_api']

    def cut_ad_content(self, title, content):

        if title is None or content is None:
            return {}
        content_set = [x for x in jieba.analyse.extract_tags(title + content + title + title + title,
                                                             topK=max(80, len(content) / 4), withWeight=True)]
        # sum_value = sum([x[1] for x in content_set])
        to_one_dict = dict()
        para = len(content_set) / 0.01
        for all_set in content_set:
            tmp_weight = all_set[1] * para
            if tmp_weight > 600:
                to_one_dict[all_set[0]] = int(tmp_weight)
        # 前8
        tmp_dict = dict(sorted(to_one_dict.items(),
                               key=lambda d: d[1], reverse=True)[:8])
        tmp_sum = sum([x[1] for x in tmp_dict.items()])
        new_dict = {}
        for k, v in tmp_dict.items():
            k = k.replace('.', '```')
            k = k.replace('$', '%^&')
            new_dict[k] = 1.0 * v / tmp_sum
        return new_dict

    def time_decay(self, tags, tags_new, top_category, category, city, ts, ts_now):

        two_day_s = 172800
        if ts_now < ts:
            ts_now = ts
        times = math.pow(0.5, (ts_now - ts) / two_day_s)
        try:
            tags.setdefault(top_category, {})
            tags[top_category].setdefault(category, {})
            tags[top_category][category].setdefault('content', {})

            for k1, v1 in tags.items():
                for k2, v2 in v1.items():
                    for k3, v3 in v2['content'].items():
                        tags[k1][k2]['content'][k3] = v3 * times
                        if k1 == top_category and k2 == category:
                            for k3_new, v3_new in tags_new.items():
                                k3_new = k3_new.replace('.', '```')
                                k3_new = k3_new.replace('$', '%^&')
                                tags[k1][k2]['content'].setdefault(k3_new, 0)
                                tags[k1][k2]['content'][k3_new] += v3_new
                            tags[k1][k2]['content'] = dict(
                                sorted(tags[k1][k2]['content'].items(), key=lambda d: d[1], reverse=True)[:50])

        except KeyError as e:
            logging.error(e)
            return tags
        # 前 50
        return tags

    def count_online_tags(self, value):

        # 1.尝试从mongo取标签
        user_id = value['udid'].encode('utf-8')
        ad_id = value['adid'].encode('utf-8')
        ts_now = value['interview_time']
        # city = value['city']
        # category = value['category']
        mongo_driver = Mongo('chaoge', 0)
        mongo_driver.connect()
        result = mongo_driver.read('ad_content', {'_id': ad_id})

        # 2.如果mongo有就用mongo，如果没有就取mysql，切ad 并存入mongo
        try:
            result = result.next()
            tags_new = result['tags']
            top_category = result['top_category']
            category = result['category']
            #city = result['city']
            #print('read from db')
            city = ''

        except (StopIteration, KeyError):
            get_ad_info_url = self.mysql_api.format(ad_id)
            #print('read from ad url')
            try:
                request_info = json.loads(requests.get(get_ad_info_url).text)
            except Exception as e:
                logging.error(e)
                return
            title, ad_content = request_info['title'], request_info['content']
            city, top_category, category = request_info['city'], request_info[
                'top_category'].encode('utf-8'), request_info['category'].encode('utf-8')
            tags_new = self.cut_ad_content(title, ad_content)
           # mongo_driver.insert('ad_content', [{'_id': ad_id, 'city': city, 'top_category': top_category,
           #                                     'category': category, 'update_time': ts_now, 'tags': tags_new}])
            mongo_driver.update('ad_content', '_id', {
                                '_id': ad_id, 'city': city, 'top_category': top_category, 'category': category, 'update_time': ts_now, 'tags': tags_new})

        # 3.写redis
        bf = Bf()
        bf.save(user_id, ad_id, method='view')

        # 4.拿到标签并更新在线部分
        online_result = mongo_driver.read(
            'RecommendationUserTagsOnline', {'_id': user_id})
        try:
            online_result = online_result.next()
            tags = online_result['tags']
            ts = online_result['update_time']
        except StopIteration:
            tags = {}
            ts = ts_now
        tags = self.time_decay(
            tags, tags_new, top_category, category, city, ts, ts_now)
        mongo_driver.update('RecommendationUserTagsOnline', '_id', {
                            '_id': user_id, 'update_time': ts_now, 'tags': tags})

    def start_one_consumer(self):

        consumer = KafkaConsumer(self.topic,
                                 group_id=self.group_id,
                                 bootstrap_servers=self.host,
                                 # consumer_timeout_ms=self.timeout
                                 )
        for index, message in enumerate(consumer):
            try:
                tmp_json = json.loads(message.value)
            except Exception as e:
                logging.error(e)
                continue
            if tmp_json['type'] == 'app_vad_traffic':
                if index % 1000 == 0:
                    print "index:{0}, time:{1}, unix_time:{2}, unix_time_now:{3}".format(index, time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(tmp_json['msg']['interview_time'])), tmp_json['msg']['interview_time'], time.time())
                self.count_online_tags(tmp_json['msg'])
                if index % 1000 == 0:
                    print "unix_time_now:{}".format(time.time())
                

    def build_consumer(self):
        for num in range(self.consume_num):
            self.consumer.append(threading.Thread(
                target=self.start_one_consumer))
            self.consumer[num].daemon = True
            self.consumer[num].start()
        self.consumer[num].join()

    def test(self):
        self.build_consumer()


def test():
    a = KafkaUlConsumer()
    a.test()

test()
