import json
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

    def build_consume(self):
        for num in range(self.consume_num):
            self.consume_num[num] = KafkaConsumer(self.topic,
                                                  group_id=self.group_id,
                                                  bootstrap_servers=self.host)

    def start_consume(self):
        for x in range(self.consume_num):
            for message in self.consume_num[x]:
                # message value and key are raw bytes -- decode if necessary!
                # e.g., for unicode: `message.value.decode('utf-8')`
                print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                                      message.offset, message.key,
                                                      message.value))

    def test(self):
        self.build_consume()
        self.start_consume()

def test():
    a = KafkaUlConsumer()
    a.test()
