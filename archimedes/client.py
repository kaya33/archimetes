#!/usr/bin/env python
# -*-coding:utf-8-*-

__author__ = 'xujiang@baixing.com'

import os
import sys

sys.path.append('gen-py')
sys.path.insert(0, os.path.abspath('..'))

reload(sys)
sys.setdefaultencoding( "utf-8" )

from archimedes import logger

from recommender import Recommender
from recommender.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol,TCompactProtocol
from conf.config_default import configs

log = logger.getLogger(__name__)


# Add classes / functions as required here
def getRecServerPort():
    # In this function read the configuration file and get the port number for the server
    log.info("Get the server port by config file")
    try:
        port = int(configs.get('server').get('port', 9090))
        return port
    # Exit if you did not get blockserver information
    except Exception as e:
        log.error("cannot read server port.")
        exit(1)


def getRecServerSocket(port):
    # This function creates a socket to block server and returns it

    # Make socket
    transport = TSocket.TSocket('localhost', port)
    # Buffering is critical. Raw sockets are very slow
    transport = TTransport.TBufferedTransport(transport)
    # Wrap in a protocol
    protocol = TCompactProtocol.TCompactProtocol(transport)
    # Create a client to use the protocol encoder
    client = Recommender.Client(protocol)

    # Connect!
    #print "Connecting to block server on port", port
    try:
        transport.open()
    except Exception as e:
        log.error("Exception while connecting to block server, check if server is running on port", port)
        exit(1)

    return client

def fetchRecByItem(sock, req):
    try:
      resp = sock.fetchRecByItem(req)
      print(resp)
    except Exception as e:
      log.error("ERROR while calling fetchRecByItem")
      log.error(e)
      print("ERROR")
    if resp.status == responseType.OK:
      #print "Deletion of block successful"
      log.info("OK")
    else:
      #print "Deletion of block not successful"
      log.error("ERROR")
   # else:
       # print "Server said ERROR,  Meta server get list unsuccessful"


def fetchRecByUser(sock, req):
    try:
      resp = sock.fetchRecByUser(req)
      print(resp)
    except Exception as e:
        print e
        log.error("ERROR while calling fetchRecByUser")
        log.error(e)
        print("ERROR")
    if resp.status == responseType.OK:
      #print "Deletion of block successful"
      log.info("OK")
    else:
      #print "Deletion of block not successful"
      log.error("ERROR")
   # else:
       # print "Server said ERROR,  Meta server get list unsuccessful"


def main():
    if len(sys.argv) < 2:
        log.error("Invocation : <executable> <config_file> <command> <item_id/user_id>")
        exit(-1)
    command = sys.argv[1]
    id = sys.argv[2]

    # Make socket
    servPort = getRecServerPort()
    sock = getRecServerSocket(servPort)
    # Wrap in a protocol

    res_ = sock.ping()
    log.info(res_)


    if command == 'fetchRecByItem':
        req = ItemRequest()
        req.ad_id = id
        req.size = 4
        rec_ = fetchRecByItem(sock, req)

        print(rec_)
    elif command == 'fetchRecByUser':
        req = UserRequest()
        req.user_id = id
        req.city_name = 'shanghai'
        req.first_cat = '服务'
        req.second_cat = '丽人服务'
        rec_ = fetchRecByUser(sock, req)

if __name__ == "__main__":
    main()