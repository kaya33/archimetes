#!/usr/bin/env python
#-*- coding: utf-8 -*-

import logging
import os
import sys

sys.path.append('./gen-py')

from recommenderservice.ttypes import *
from recommenderservice import Recommender


from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol,TJSONProtocol,TCompactProtocol
from thrift.server import TServer
from api.mongo import Mongo

logger = logging.getLogger(__name__)

mongo = Mongo('chaoge')
mongo.connect()

def fetch_batch_itemrec(item_id, rec_name = "itemCF", id_type = "1"):
    data = mongo.read("RecommendationAd",{"_id":item_id+"&"+rec_name+"&"+id_type}).next()
    item_list = data['ads']
    for sub in item_list:
        for key in sub:
            sub[key] = str(sub[key])
    print(item_list)
    return item_list


class RecommenderServerHandler(object):
    def __init__(self, configpath):
        self.config_path = configpath
        self.port = self.readServerPort()

    def ping(self):
        return 'ping()'

    def readServerPort(self):
        # In this function read the configuration file and get the port number for the server
        print("Checking validity of the config path")
        if not os.path.exists(self.config_path):
            print("ERROR: Config path is invalid")
            exit(1)
        if not os.path.isfile(self.config_path):
            print("ERROR: Config path is not a file")
            exit(1)

        print("Reading config file")
        with open(self.config_path, 'r') as conffile:
            lines = conffile.readlines()
            for line in lines:
                if 'recamendationAd' in line:
                    # Important to make port as an integer
                    return int(line.split()[1].lstrip().rstrip())

        # Exit if you did not get blockserver information
        print("ERROR: rec server information not found in config file")
        exit(1)

    def fetchRecByItem(self,req):
        print(req)
        print("get the rec response by item ...")

        res = RecResponse()
        res.status = responseType.OK
        res.errStr = ""
        res.data = []
        try:
        # TODO 调用离线推荐列表数据
            if req.item_id is None:
                res.status = responseType.ERROR;
                res.errStr = "item_id不能为空"
                res.data = []
                return res

            item_id = req.item_id
            data = fetch_batch_itemrec(item_id)
            res.data.extend(data)


            # TODO 调用用户画像数据
            res.data.extend([])

            # TODO 排序

            # TODO 过滤

        except Exception as e:
            print(e)
            logger.error(e)

        return res

def main():
    if len(sys.argv) < 2:
        print("Invocation <executable> <config_file>")
        exit(-1)

    config_path = sys.argv[1]

    print("Initializing recamendation server")
    handler = RecommenderServerHandler(config_path)
    port = handler.readServerPort()
    processor = Recommender.Processor(handler)
    transport = TSocket.TServerSocket(port=port)
    tfactory = TTransport.TBufferedTransportFactory()
    pfactory = TCompactProtocol.TCompactProtocolFactory()
    server = TServer.TThreadedServer(processor, transport, tfactory, pfactory)
    print("Starting server on port : ", port)

    try:
        server.serve()
    except (Exception, KeyboardInterrupt) as e:
        print("\nExecption / Keyboard interrupt occured: ", e)
        exit(0)

if __name__ == '__main__':
    main()