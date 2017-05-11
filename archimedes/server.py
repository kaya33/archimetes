#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'xujiang@baixing.com'


import os
import sys


sys.path.append('./gen-py')

import logger

from recommenderservice.ttypes import *
from recommenderservice import Recommender


from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol,TJSONProtocol,TCompactProtocol
from thrift.server import TServer
from api.mongo import Mongo
from core.combine_sort import sample_sort
from conf.config_default import configs
from core.fetch_item_result import fetch_batch_userrec,fetch_batch_itemrec,fetchKwData



log = logger.getLogger(__name__)

mongo = Mongo('chaoge')
mongo.connect()

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
            log.error(e)

        # TODO 排序
        combine_data = sample_sort(data)
        print combine_data
        # TODO 过滤

        for sub in combine_data:
            for key in sub:
                sub[key] = str(sub[key])
        res.data.extend(combine_data[:size])
        return res


    def fetchRecByUser(self,req):
        print(req)
        print("get the rec response by item ...")

        res = RecResponse()
        res.status = responseType.OK
        res.errStr = ""
        res.data = []

        if req.user_id is None:
            res.status == responseType.ERROR
            res.errStr = "获取离线推荐数据失败"
            res.data = []
            return res

        user_id = req.user_id
        city = req.city
        category = req.category
        if req.size > 0:
            size = req.size
        else:
            size = 3

        try:
            # get user key word
            kw = fetch_batch_userrec(user_id,city,category)
            data = {"num": 3, "city": city, "category": category, "tag": kw, "days": 400}
            # get user_profile data by kw
            response = fetchKwData(data)
            return res
        except Exception as e:
            res.status = responseType.ERROR
            res.errStr = ""
            log.error(e)

        # TODO 调用离线推荐列表数据

        # TODO 排序
        combine_data = sample_sort(data)

        # TODO 过滤

        for sub in combine_data:
            for key in sub:
                sub[key] = str(sub[key])
        res.data.extend(combine_data[:size])

def main():

    log.info("Initializing recamendation server")
    handler = RecommenderServerHandler()
    port = handler.getServerPort()
    number_thread = handler.getNumThread()
    processor = Recommender.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()
    server = TServer.TThreadPoolServer(processor, transport, tfactory, pfactory).setNumThreads(number_thread)
    log.info("Starting server on port : {}".format(str(port)))
    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        log.error("Execption / Keyboard interrupt occured: ", e)
        exit(0)

if __name__ == '__main__':
    main()