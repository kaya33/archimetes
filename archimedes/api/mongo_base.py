# -*- coding: UTF-8 -*-
import pymongo
import json
import logging
from conf.config_default import configs
from pymongo import MongoClient, ReplaceOne

logging.basicConfig(level=logging.WARNING, format="%(asctime)s-%(name)s-%(levelname)s-%(message)s")

class Singleton(type):

    _instances = {}

    def __call__(cls, *args, **kwargs):
        if cls not in cls._instances:
            cls._instances[cls] = super(Singleton, cls).__call__(*args, **kwargs)
        return cls._instances[cls]


class Mongo():

    # python2 metaclass
    __metaclass__ = Singleton

    def __init__(self, db_name, is_prod=1):

        self.db_name = db_name
        self.read_db = None
        self.write_db = None

        if is_prod:
            self.read_uri = configs.get('mongo').get('prod_read_uri')
            self.write_uri = configs.get('mongo').get('prod_write_uri')

        else:
            self.read_uri = configs.get('mongo').get('dev_read_uri')
            self.write_uri = configs.get('mongo').get('dev_write_uri')

    def connect(self):
        if self.read_db is None:
            self.read_db = MongoClient(self.read_uri)[self.db_name]
        if self.write_db is None:
            self.write_db = MongoClient(self.write_uri)[self.db_name]

    def reconnect(self):
        # release
        try:
            self.close_connect()
        except:
            pass
        self.read_db = MongoClient(self.read_uri)[self.db_name]
        self.write_db = MongoClient(self.write_uri)[self.db_name]

    def close_connect(self):

        if self.read_db is not None:
            self.read_db.close()
        if self.write_db is not None:
            self.write_db.close()

    def read(self, collect_name, search_json={}):

        if len(search_json) == 0:
            logging.warning('[mongo]searching without term is not allowed')
            return iter([])
        if self.read_db[collect_name]:
            query_data = self.read_db[collect_name]
            return query_data.find(search_json)
        else:
            logging.warning('[mongo]read without connect')
            return iter([])

    def insert(self, collect_name, data):

        if len(data) == 0:
            logging.warning('insert data is empty')
            return []

        query_obj = self.write_db[collect_name]
        try:
            result = query_obj.insert_many(data)
            return result
        except Exception as e:
            logging.error('[mongo]insert err, collect_name:{0}, data:{1}, err:{2}'.format(collect_name, data, e))
            return []

    def delete(self, collect_name, search_json={}):

        try:
            res = self.write_db[collect_name].remove(search_json, safe=True)
            assert res['n'] != 0
        except Exception as e:
            logging.error('[mongo]delete err, collect_name:{0}, search_json:{1}, err:{2}'.format(collect_name, search_json, e))

    def expire(self, collect_name, sec):

        query_obj = self.write_db[collect_name]
        try:
            query_obj.create_index('update_time', expireAfterSeconds=sec)
        except Exception as e:
            logging.error('[mongo]create index err, collect_name:{0}, err:{1}'.format(collect_name, e))
        return 'success'

    def update(self, collect_name, key, data):

        if type(data) == dict:
            data = [data]
        requests = []
        coll = self.write_db[collect_name]
        for d in data:
            #print d
            requests.append(ReplaceOne({key: d[key]}, d, upsert=True))

        try:
            res = coll.bulk_write(requests, ordered=False)
            return res.upserted_ids
        except Exception as e:
            logging.error('[mongo]update err, collect_name:{0}, data:{1}, err:{2}'.format(collect_name, data, e))


def test():
    a = Mongo('chaoge')
    b = Mongo('chaoge')
    print(a == b)
    print(a is b)
    print(id(a))
    print(id(b))
    # print a.test()
    # print(a.read('RecommendationAd').next())
    # print([x for x in a.insert('RecommendationUserTagsOffline', [{'user_id': '3a64b7666eca18fa',
    #                                                              'tags': {u"服务": {u"家电维修": {u"空调维修": 2, u"葫芦岛": 2}}}}])])

# test()
