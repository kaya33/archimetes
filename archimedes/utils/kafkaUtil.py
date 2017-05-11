#!/usr/bin/env python
# -*-coding:utf-8-*-

__author__ = 'xujiang@baixing.com'

import time
import json

from kafka import KafkaProducer
from kafka.errors import KafkaError

from archimedes.conf.config_default import configs
from archimedes import logger

class kafkaProduct(object):

    def __init__(self,server, group_id):
        self.log = logger.getLogger(__name__)
        self.server = server
        self.group_id =group_id

    def send(self, message):
        """
        :param message: json 
        :return: 
        """
        producer = KafkaProducer(bootstrap_servers=self.server, value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        try:
            producer.send(self.group_id, message)
        except Exception as e:
            self.log.error("kafka produce err .", e)


if __name__ == "__main__":
    producer = kafkaProduct(configs.get('kafka').get('host'), configs.get('kafka').get('group_id'))
    producer.send({
    "type": "archimedes",
    "ts": int(round(time.time())*1000),
    "msg": {
        "stage": "test",
        "duration": 29.127197265625,
        "url": "http://jianglife.com",
        "controller": "test",
        "action": "index",
        "page": "home",
        "platform": "mobile",
        "request_id": "591277f1c89a317074"
    }
})







