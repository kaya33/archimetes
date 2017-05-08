# -*- coding: UTF-8 -*-
import pymongo
from pymongo import MongoClient


class Mongo():

    def __init__(self, db_name):

        self.read_db = MongoClient('mongodb://ro:ReadOnly@192.168.1.40:27017')[db_name]
        self.write_db = MongoClient('mongodb://admin:SuperPower@192.168.1.40:27017')[db_name]

    def read(self, collect_name, search_json={}):

        query_data = self.read_db[collect_name]
        return query_data.find(search_json)

    def insert(self, collect_name, data):

        if len(data) == 0:
            return

        query_obj = self.write_db[collect_name]
        return query_obj.insert_many(data)

def test():
    a = Mongo('chaoge')
    print(a.read('RecommendationAd').next())
    # print([x for x in a.insert('RecommendationUserTagsOffline', [{'user_id': '3a64b7666eca18fa',
    #                                                              'tags': {u"服务": {u"家电维修": {u"空调维修": 2, u"葫芦岛": 2}}}}])])

# test()