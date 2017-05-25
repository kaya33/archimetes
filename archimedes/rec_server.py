#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'xujiang@baixing.com'

import random
import os
import sys
import multiprocessing
import json
import datetime

sys.path.append('./gen-py')

import logger
import time
import re

from recommender.ttypes import *
from recommender import Recommender

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol,TJSONProtocol,TCompactProtocol
from thrift.server import TServer
from api.mongo_base import Mongo
from api.user_tag import UserProfile
from api.bloom_filter import BloomFilter
from core.combine_sort import sample_sort
from api.redis_ul import RedisUl
from conf.config_default import configs
from data.base_data_source import fetchKwData

log = logger.getLogger(__name__)

reload(sys)
sys.setdefaultencoding( "utf-8" )

def fetch_batch_itemrec(ad_id, rec_name = "itemCF", id_type = "1",size=3):
    data = mongo.read("RecommendationAd",{"_id":ad_id+"&"+rec_name+"&"+id_type}).next()
    item_list = data['ads'][:size]
    for item in item_list:
        item['rec_name'] = 'itemCF'
    return item_list


def fetch_batch_userrec(user_id,first_category,second_category,city=None,size=3):
    """
    :param user_id: 
    :param first_category: 
    :param second_category: 
    :param city: 
    :param size: post size to return
    :return: [{'rec_id':rec_id,'sim':sim,'rec_name':rec_name},{} ...]
    """

    result_list = []

    # get tags size from config
    kw_size = int(configs.get('user_profile').get('kw_size',4))
    print kw_size
    try:
        off_tag_data = up.read_tag('RecommendationUserTagsOffline', {'_id':user_id}, top=size)
    except Exception as e:
        off_tag_data = {}

    try:
        on_tag_data = up.read_tag('RecommendationUserTagsOnline', {'_id': user_id}, top=size)
    except Exception as e:
        on_tag_data = {}

    print on_tag_data

    contact_tags = []
    online_tags = []
    total_tag = []
    try:
        contact_tags = off_tag_data[first_category][second_category]['contact_mata'][:(kw_size*10)]
    except KeyError:
        pass
    try:
        online_tags = on_tag_data[first_category][second_category]['content'][:(kw_size*10)]
    except KeyError:
        pass
    print len(contact_tags), kw_size*10
    if len(contact_tags) >= kw_size*10 and len(online_tags)>=kw_size*10:
        total_tag = contact_tags[:(kw_size*10)] + online_tags[:(kw_size*10)]
    elif len(contact_tags) < kw_size*10:
        contact_tags_size = len(contact_tags)
        total_tag = contact_tags[:contact_tags_size] + online_tags[:(kw_size*10 - contact_tags_size)]
    else:
        contact_tags_size = online_tags[:(kw_size*10)]
        total_tag = online_tags[:contact_tags_size] + contact_tags[:(kw_size*20 - contact_tags_size)]
    if len(total_tag) < kw_size*20:
        try:
            print first_category, second_category
            print off_tag_data
            total_tag_size = len(total_tag[:(kw_size*10)])
            total_tag += off_tag_data[first_category][second_category]['mata'][:(kw_size*20 - total_tag_size)]
            total_tag += off_tag_data[first_category][second_category]['content'][:(kw_size*20 - len(total_tag))]
        except KeyError:
            print '获取用户标签数据为空'

    # if tag is None the return
    if len(total_tag) == 0:
        log.warning("获取关键词为空！")
        return result_list
    
    print total_tag
    # 根据用户标签来获取帖子
    try:
        tmp_list = []
        for info_tuple in total_tag:
            k, v = info_tuple
            k = k.encode('utf-8')
            v = float(v)
            tmp_list.append((k, v))

        tmp_list_sample = random.sample(tmp_list, kw_size)
        second_category = second_category.encode('utf-8')
        begin = datetime.datetime.now()
        kwdata = {"num": size,"city": city,"category": second_category,"tag": "_".join([x[0] for x in tmp_list_sample]),"weight":[x[1] for x in tmp_list_sample],"days": 60, 'cut':1000}
        print kwdata
        user_profile_ad = fetchKwData(kwdata)
        if len(user_profile_ad)<size:
            kwdata = {"num": size,"city": city,"category": second_category,"tag": "_".join([x[0] for x in tmp_list_sample]),"weight":[x[1] for x in tmp_list_sample],"days": 270, 'cut':1000}
            user_profile_ad.extend(fetchKwData(kwdata))
        end = datetime.datetime.now()
        print "get ad_list by user tag cost time %s sec\n" % (end - begin)
    except Exception as e:
        log.error("获取用户画像失败, {}".format(e))
        user_profile_ad = []

    for info_tuple in user_profile_ad:
        k, v = info_tuple['ad_id'], info_tuple['score']
        result_list.append(({"rec_id":k, "sim":v, "rec_name":"user_profile"}))
    print result_list
    return result_list

mongo = Mongo('chaoge', 0)
up = UserProfile('chaoge', 0)
mongo.connect()
up.connect()

class RecommenderServerHandler(object):
    def __init__(self):
        pass

    def ping(self):
        return 'ping success !'

    def fetchRecByItem(self,req):
        print(req)
        print("get the rec response by item ...")

        res = RecResponse()
        res.status = responseType.OK
        res.err_str = ""
        res.data = []

        if req.ad_id is None:
            res.status = responseType.ERROR;
            res.err_str = "ad_id不能为空"
            res.data = []
            return res

        ad_id = req.ad_id
        if req.size > 0:
            size = req.size
        else:
            size = 3
        try:
            # get offline recommender data
            data = fetch_batch_itemrec(ad_id.encode('utf-8'))
        except Exception as e:
            res.status = responseType.ERROR
            res.err_str = "获取离线推荐数据失败"
            return res

        combine_data = sample_sort(data)

        for obj in combine_data[:size]:
            res.data.append(OneRecResult(obj['rec_id'],'itemCF'))

        return res


    def fetchRecByUser(self,req):
        print(req)
        log.info("get the rec response by user ...")
        res = RecResponse()
        res.status = responseType.OK
        res.err_str = ""
        res.data = []

        if req.user_id is None:
            res.status == responseType.ERROR
            res.err_str = "用户ID不能为空"
            res.data = []
            return res

        user_id = req.user_id.encode('utf-8')
        city = req.city_name.encode('utf-8')
        first_category = req.first_category
        second_category = req.second_category
        if req.size > 0:
            size = req.size
        else:
            size = 3
        try:
            # get user key word
            print 'fetch batch user rec'
            data = fetch_batch_userrec(user_id=user_id,first_category=first_category,second_category=second_category,city=city,size=100)
        except Exception as e:
            res.status = responseType.ERROR
            res.err_str = "获取用户画像数据失败"
            return res


        # TODO 调用离线推荐列表数据

        print 'data ',data
        if len(data) == 0:
            log.error("用户画像数据为空")
            res.status = responseType.ERROR
            res.err_str = "用户画像数据为空"
            return res

        combine_data = sample_sort(data)
        print 'combine data', combine_data
        # TODO bloom 过滤
        bf = BloomFilter()
        combine_data = bf.filter_ad_by_user(user_id, combine_data)
        bf.save(user_id, [x[0] for x in combine_data][:size], 'rec')

        for obj in combine_data[:size]:
            res.data.append(OneRecResult(str(obj['rec_id']),'user_profile'))
        return res

    def fetchRecByMult(self,req):
        """ Integration of multiple strategies
        :param req: 
        :return: 
        
        """
        res = RecResponse()
        res.status = responseType.OK
        res.err_str = ""
        res.data = []

        if req.user_id is None or req.ad_id is None:
            res.status == responseType.ERROR
            res.err_str = "用户ID和ad_id不能为空"
            res.data = []
            return res

        user_id = req.user_id
        ad_id= req.ad_id
        city = req.city_name.encode('utf-8')
        first_category = req.first_category
        second_cat = req.second_cat
        try:
            size = req.size
        except:
            size = 3

        # get user tags
        results = []
        try:
            pool = multiprocessing.Pool(processes=2)
            results.append(pool.apply_async(fetch_batch_userrec, (user_id,first_category,second_cat,city,size,)))
            results.append(pool.apply_async(fetch_batch_itemrec,(ad_id,size,)))
            pool.close()
            pool.join()
        except Exception as e:
            res.status == responseType.ERROR
            res.err_str = "获取推荐结果失败"
            res.data = []
            return res

        result = []

        for obj in results[0].get():
            res.data.append(OneRecResult(obj['rec_id'], 'user_profile'))
        for obj in results[1].get():
            res.data.append(OneRecResult(obj['rec_id'], 'itemCF'))
        # TODO 协同过滤，优先

        if len(res.data) == 0:
            res.status == responseType.ERROR
            res.err_str = "推荐结果为空"
            res.data = []
            return res
        return res

def main():
    log.info("Initializing recamendation server")
    handler = RecommenderServerHandler()
    port = int(configs.get('server').get('port',9090))
    number_thread = int(configs.get('server').get('thread',5))
    processor = Recommender.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory)
    log.info("Starting server on port : {}".format(str(port)))
    try:
        server.setNumThreads(number_thread)
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print e
        # log.error("Execption / Keyboard interrupt occured: ", e)
        log.error(e)
        exit(0)

if __name__ == '__main__':
    main()
