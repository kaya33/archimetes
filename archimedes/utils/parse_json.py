#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'xujiang@baixing.com'



result = set()

def list_all_dict(dict_a):
    if isinstance(dict_a, dict):  # 使用isinstance检测数据类型
        for x in range(len(dict_a)):
            temp_key = dict_a.keys()[x]
            result.add(temp_key)
            temp_value = dict_a[temp_key]
            list_all_dict(temp_value)  # 自我调用实现无限遍历

def get_all_keywd(dict_a):
    list_all_dict(dict_a)
    return result


if __name__ == "__main__":
    from archimedes.api.mongo import Mongo

    mongo = Mongo('chaoge', 0)
    mongo.connect()
    data = mongo.read('RecommendationUserTagsOffline', {'user_id': "0195353beaf444fcbe9776d3772f5daa"}).next()
    print data['tags']
    result = get_all_keywd(data['tags'])
    print result
