import json
import threading
import requests
import logging
import jieba.analyse
from bloom_filter import Bf
from mongo_base import Mongo
from kafka import KafkaConsumer

class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class KafkaUlConsumer():

    # __metaclass__ = Singleton

    def __init__(self):

        conf = json.load(open('conf/kafka_conf.json'))
        self.consumer = []

        self.host = conf['host']
        self.group_id = conf['group_id']
        self.timeout = conf['time_out']
        self.topic = conf['topic']
        self.consume_num = conf['consume_num']

    def cut_ad_content(self, title, content):
        content_set = [x for x in jieba.analyse.extract_tags(title + content + title + title + title,
                                                             topK=max(80, len(content) / 4), withWeight=True)]
        # sum_value = sum([x[1] for x in content_set])
        to_one_dict = dict()
        para = len(content_set) / 0.01
        for all_set in content_set:
            tmp_weight = all_set[1] * para
            if tmp_weight > 600:
                to_one_dict[all_set[0]] = int(tmp_weight)

        return to_one_dict

    def count_online_tags(self, value):

        # 1.尝试从mongo取标签
        user_id = value['user_id']
        ad_id = value['ad_id']
        city = value['city']
        category = value['category']
        mongo_driver = Mongo('chaoge', 0)
        mongo_driver.connect()
        result = mongo_driver.read('ad_content', {'_id': ad_id})

        # 2.如果mongo有就用mongo，如果没有就取mysql，切ad 并存入mongo
        try:
            kws = result.next()['kws']
        except:
            get_ad_info_url = 'http://www.baixing.com/recapi/getAdInfoById?adId={}'.format(ad_id)
            try:
                request_info = json.loads(requests.get(get_ad_info_url).text)
            except Exception as e:
                logging.error(e)
                return
            title, ad_content = request_info['title'], request_info['content']
            kws = self.cut_ad_content(title, ad_content)

        # 3.写redis
        bf = Bf()
        bf.save(user_id, ad_id, method='view')


        # 4.拿到标签并更新在线部分
        pass

    def start_one_consumer(self):

        consumer = KafkaConsumer(self.topic,
                                 group_id=self.group_id,
                                 bootstrap_servers=self.host,
                                 consumer_timeout_ms=self.timeout)
        for message in consumer:
            # message value and key are raw bytes -- decode if necessary!
            # e.g., for unicode: `message.value.decode('utf-8')`
            if message.topic == 'app_vad_traffic':
                self.count_online_tags(message.value)

    def build_consumer(self):

        for num in range(self.consume_num):
            self.consumer[num] = threading.Thread(target=self.start_one_consumer)
            self.consumer[num].daemon = True
            self.consumer[num].start()


    def test(self):
        self.build_consumer()

def test():
    a = KafkaUlConsumer()
    a.test()
