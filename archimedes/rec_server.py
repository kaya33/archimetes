#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'xujiang@baixing.com'


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
from api.user_tag import UP
#from api.bloom_filter import Bf
from core.combine_sort import sample_sort,sample_sort1
from conf.config_default import configs

from utils.parse_json import get_all_keywd
from data.base_data_source import fetchKwData

log = logger.getLogger(__name__)

reload(sys)
sys.setdefaultencoding( "utf-8" )

def fetch_batch_itemrec(ad_id, rec_name = "itemCF", id_type = "1",size=3):
    data = mongo.read("RecommendationAd",{"_id":ad_id+"&"+rec_name+"&"+id_type}).next()
    item_list = data['ads']
    return item_list[:10]


def fetch_batch_userrec(user_id,first_cat,second_cat,city=None,size=3):
    try:
        on_tag_data = up.read_tag('RecommendationUserTagsOffline', {'_id':user_id}, top=size)
    except Exception as e:
        on_tag_data = {}

    try:
        off_tag_data = up.read_tag('RecommendationUserTagsOnline', {'_id': user_id}, top=size)
    except Exception as e:
        off_tag_data = {}

    print off_tag_data

    try:
        contact_tags = None
        online_tag = None
        total_tag = []
        try:
            contact_tags = off_tag_data[first_cat][second_cat]['contact_mata'][:4]
        except KeyError:
            pass
        try:
            online_tag = on_tag_data[first_cat][second_cat]['content'][:4]
        except KeyError:
            pass
        if contact_tags:
            total_tag += contact_tags
        if on_tag_data and len(total_tag) > 0:
            total_tag = total_tag[:2] + online_tag[:2]
        elif on_tag_data:
            total_tag += online_tag
        if len(total_tag) == 4:
            try:
                total_tag += off_tag_data[first_cat][second_cat]['content'][:4]
            except KeyError:
                pass
    except Exception as e:
        log.error("用户标签失败, {}".format(e))
        return []

    # 根据用户标签来获取帖子
    try:
        tmp_list = []
        for info_tuple in total_tag:
            k, v = info_tuple
            k = k.encode('utf-8')
            v = float(v)
            tmp_list.append((k, v))
        second_cat = second_cat.encode('utf-8')
        begin = datetime.datetime.now()
        kwdata = {"num": size,"city": city,"category": second_cat,"tag": "_".join([x[0] for x in tmp_list]),"weights":[x[1] for x in tmp_list],"days": 60}
        user_profile_ad = fetchKwData(kwdata)
        print user_profile_ad
        if len(user_profile_ad)<size:
            kwdata = {"num": size,"city": city,"category": second_cat,"tag": "_".join([x[0] for x in tmp_list]),"weights":[x[1] for x in tmp_list],"days": 270}
            user_profile_ad.extend(fetchKwData(kwdata))
        end = datetime.datetime.now()
        print "get ad_list by user tag cost time %s sec\n" % (end - begin)
    except Exception as e:
        log.error("获取用户画像失败, {}".format(e))
        user_profile_ad = []

    tmp_list = []
    for info_tuple in user_profile_ad:
        k, v = info_tuple['ad_id'], info_tuple['score']
        tmp_list.append(({"rec_id":k, "sim":v}))
    return tmp_list


mongo = Mongo('chaoge', 0)
up = UP('chaoge', 0)
mongo.connect()
up.connect()

class RecommenderServerHandler(object):
    def __init__(self):
        pass

    def ping(self):
        return 'ping()'

    def getServerPort(self):
        # In this function read the configuration file and get the port number for the server
        log.info("Get the server port by config file")
        try:
            port = int(configs.get('server').get('port',9090))
            return port
        # Exit if you did not get blockserver information
        except Exception as e:
            log.error("cannot read server port.")
            exit(1)

    def getNumThread(self):
        # In this function read the configuration file and get the port number for the server
        log.info("Get the server thread num by config file")
        try:
            port = int(configs.get('server').get('thread',5))
            return port
        # Exit if you did not get blockserver information
        except Exception as e:
            log.error("cannot read server thread number.")
            exit(1)


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
        print("get the rec response by user ...")

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
        first_cat = req.first_cat
        second_cat = req.second_cat
        if req.size > 0:
            size = req.size
        else:
            size = 3
        try:
            # get user key word
            print 'fetch batch user rec'
            data = fetch_batch_userrec(user_id=user_id,first_cat=first_cat,second_cat=second_cat,city=city,size=100)
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
        # TODO bloom 过滤
        # bf = Bf()
        # combine_data = bf.filter_ad_by_user(user_id, combine_data)
        # bf.save(user_id, [x[0] for x in combine_data][:size], 'rec')

        for obj in combine_data[:size]:
            res.data.append(OneRecResult(str(obj['rec_id']),'user_prifile'))
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
        first_cat = req.first_cat
        second_cat = req.second_cat
        try:
            size = req.size
        except:
            size = 3

        # get user tags
        results = []
        try:
            pool = multiprocessing.Pool(processes=4)
            results.append(pool.apply_async(fetch_batch_userrec, (user_id,first_cat,second_cat,city,3,)))
            results.append(pool.apply_async(fetch_batch_itemrec,(ad_id,)))
            pool.close()
            pool.join()
        except Exception as e:
            res.status == responseType.ERROR
            res.err_str = "获取推荐结果失败"
            res.data = []
            return res

        result = []
        for tmp in results:
            result.append(tmp.get()[:4])
        print result
        for obj in result[0]:
            res.data.append(OneRecResult(obj['rec_id'], 'user_profile'))
        for obj in result[1]:
            res.data.append(OneRecResult(obj['rec_id'], 'itemCF'))

        if len(res.data) == 0:
            res.status == responseType.ERROR
            res.err_str = "推荐结果为空"
            res.data = []
            return res
        return res

def main():

    log.info("Initializing recamendation server")
    handler = RecommenderServerHandler()
    port = handler.getServerPort()
    number_thread = handler.getNumThread()
    processor = Recommender.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()
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
