# -*- coding: utf-8 -*-
import logging
import sys
import time

sys.path.append('gen-py')

from recommender import Recommender # 导入thrift生成的service
from recommender.ttypes import *
from harpc import client
from harpc.common import config

import datetime

logging.basicConfig(level=logging.DEBUG,
                    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
                    filename='../logs/zk_client.log',
                    filemode='w')

logger = logging.getLogger(__name__)

def fetchRecByMult(sock, req):

    try:
        resp = sock.fetchRecByMult(req)
        print(resp)
    except Exception as e:
        print e
        logger.error("ERROR while calling fetchRecByMult")
        logger.error(e)
        print("ERROR")
    if resp.status == responseType.OK:

        #print "Deletion of block successful"
        logger.info("OK")
    else:
        #print "Deletion of block not successful"
        logger.error("ERROR")

def fetchRecByItem(sock, req):
    try:
        begin = datetime.datetime.now()
        resp = sock.fetchRecByItem(req)
        end = datetime.datetime.now()
        print(resp)
        print "the Thread sleep  %s sec\n" % ( end - begin)
    except Exception as e:
        logger.error("ERROR while calling fetchRecByItem")
        logger.error(e)
        print("ERROR")
    if resp.status == responseType.OK:
        #print "Deletion of block successful"
        logger.info("OK")
    else:
        #print "Deletion of block not successful"
        logger.error("ERROR")

def fetchRecByUser(sock, req):
    try:
        begin = datetime.datetime.now()
        resp = sock.fetchRecByUser(req)
        end = datetime.datetime.now()
        print(resp)
        print "the rec by user cost time %s sec\n" % (end - begin)
    except Exception as e:
        print e
        logger.error("ERROR while calling fetchRecByUser")
        logger.error(e)
        print("ERROR")
    if resp.status == responseType.OK:

        #print "Deletion of block successful"
        logger.info("OK")
    else:
        #print "Deletion of block not successful"
        logger.error("ERROR")
        # else:
        # print "Server said ERROR,  Meta server get list unsuccessful"

if __name__ == '__main__':
    # read config file
    conf = config.Config("./conf/client.conf") #读取配置文件
    # setting config  use zk
    # conf = config.Config() # 初始化配置文件
    # conf.set("client", "service", "python_test$EchoService") # 设置服务名，此处和server的相同
    # conf.set("client", "zk_connect_str", "172.18.1.22:2181") # zk 连接地址

    # setting config direct connect， 不通过zk 进行直连的方式连接server
    # conf = config.Config()
    # conf.set("client", "use_zk", "False")
    # conf.set("client", "direct_address", "127.0.0.1:9095") # ip：port 对应server的ip 和 端口
    manager = client.Client(Recommender.Client, conf)  #TutorialService.Client thrift生成的Client
    proxy_client = manager.create_proxy()
    req = ItemRequest()
    req.ad_id = '1024694347'
    req.size = 4
    for i in range(0, 40):
        begin = datetime.datetime.now()
        #print proxy_client.ping()
        proxy_client.fetchRecByItem(req)
        end = datetime.datetime.now()
        print "the Thread sleep  %s sec\n" % (end - begin)
        time.sleep(0.1)
    manager.close()