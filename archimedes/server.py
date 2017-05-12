#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'xujiang@baixing.com'


import os
import sys

import json


sys.path.append('./gen-py')

import logger
import re

from recommender.ttypes import *
from recommender import Recommender

from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol,TJSONProtocol,TCompactProtocol
from thrift.server import TServer
from api.mongo_base import Mongo
from api.user_tag import UP
from core.combine_sort import sample_sort,sample_sort1
from conf.config_default import configs

from utils.parse_json import get_all_keywd
from data.base_data_source import fetchKwData

log = logger.getLogger(__name__)

reload(sys)
sys.setdefaultencoding( "utf-8" )

def chinese_word(obj):
    return re.sub(r"\\u([a-f0-9]{4})", lambda mg: unichr(int(mg.group(1), 16)), json.dumps(obj))

def fetch_batch_itemrec(ad_id, rec_name = "itemCF", id_type = "1"):
    data = mongo.read("RecommendationAd",{"_id":ad_id+"&"+rec_name+"&"+id_type}).next()
    item_list = data['ads']
    return item_list


def fetch_batch_userrec(user_id,first_cat,second_cat,city=None,size=3):
    print 'get mongo data'
    data = up.read_tag('RecommendationUserTagsOffline', {'user_id':user_id}, top=size)
    ## contant key word
    tags = data[first_cat][second_cat]['contant']
    # tags = chinese_word(str(tags))
    # print tags[]
    tmp_list = []
    for info_tuple in tags:
        k, v = info_tuple
        k = k.encode('utf-8')
        v = float(v)
        tmp_list.append((k, v))
    second_cat = second_cat.encode('utf-8')
    kwdata = {"num": size,"city": city,"category": second_cat,"tag": "_".join([x[0] for x in tmp_list]),"days": 400}
    print kwdata
    user_profile_result = fetchKwData(kwdata)
    print user_profile_result
    ## meta key word
    tags = data[first_cat][second_cat]['mata']
    print tags
    tmp_list = []
    for info_tuple in tags:
        k, v = info_tuple
        k = k.encode('utf-8')
        v = float(v)
        tmp_list.append((k, v))
    print "_".join([x[0] for x in tmp_list])
    kwdata1 = {"num": size,"city":city, "category": second_cat, "tag": "_".join([x[0] for x in tmp_list]),"days": 400}
    print kwdata1
    user_profile_result.extend(fetchKwData(kwdata1))
    print user_profile_result

    tmp_list = []
    for info_tuple in user_profile_result:
        k, v = info_tuple['ad_id'], info_tuple['score']
        tmp_list.append((k, v))
    print repr(tmp_list)
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
        res.errStr = ""
        res.data = []

        if req.ad_id is None:
            res.status = responseType.ERROR;
            res.errStr = "ad_id不能为空"
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
            # TODO 调用用户画像数据

        except Exception as e:
            res.status = responseType.ERROR
            res.errStr = "获取离线推荐数据失败"
            return res


        # TODO 排序
        combine_data = sample_sort(data)
        # TODO 过滤

        for obj in combine_data[:size]:
            res.data.append(OneRecResult(obj['rec_id'],'itemCF'))

        return res


    def fetchRecByUser(self,req):
        print(req)
        print("get the rec response by user ...")

        res = RecResponse()
        res.status = responseType.OK
        res.errStr = ""
        res.data = []

        if req.user_id is None:
            res.status == responseType.ERROR
            res.errStr = "获取离线推荐数据失败"
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
            data = fetch_batch_userrec(user_id=user_id,first_cat=first_cat,second_cat=second_cat,city=city,size=100)
        except Exception as e:
            res.status = responseType.ERROR
            res.errStr = "获取用户画像数据失败"
            return res


        # TODO 调用离线推荐列表数据

        print data
        combine_data = sample_sort1(data)
        # TODO bloom 过滤


        for obj in combine_data[:size]:
            res.data.append(OneRecResult(str(obj[0]),'user_prifile'))
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
        log.error("Execption / Keyboard interrupt occured: ", e)
        exit(0)

if __name__ == '__main__':
    main()