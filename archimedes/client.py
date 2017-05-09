#!/usr/bin/env python
# -*-coding:utf-8-*-


import os
import sys

sys.path.append('gen-py')
sys.path.insert(0, os.path.abspath('..'))

from archimedes import logger

from recommenderservice import Recommender
from recommenderservice.ttypes import *

from thrift import Thrift
from thrift.transport import TSocket
from thrift.transport import TTransport
from thrift.protocol import TBinaryProtocol,TCompactProtocol

logger = logger.getLogger(__name__)


# Add classes / functions as required here
def getRecServerPort(config_path):
    # This function reads config file and gets the port for block server

    #print "Checking validity of the config path"
    if not os.path.exists(config_path):
        print("ERROR: Config path is invalid")
        exit(1)
    if not os.path.isfile(config_path):
        print("ERROR: Config path is not a file")
        exit(1)

    #print "Reading config file"
    with open(config_path, 'r') as conffile:
        lines = conffile.readlines()
        for line in lines:
            if 'recomendationAd' in line:
                # Important to make port as an integer
                return int(line.split()[1].lstrip().rstrip())

    # Exit if you did not get blockserver information
    print("ERROR: recommendationAd server information not found in config file")
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
        print("ERROR: Exception while connecting to block server, check if server is running on port", port)
        print(e)
        exit(1)

    return client

def fetchRecByItem(sock, req):
    try:
      resp = sock.fetchRecByItem(req)
      print(resp)
    except Exception as e:
      print("ERROR while calling fetchRecByItem")
      print(e)
      print("ERROR")
    if resp.status == responseType.OK:
      #print "Deletion of block successful"
      logger.info("OK")
    else:
      #print "Deletion of block not successful"
      print("ERROR")
   # else:
       # print "Server said ERROR,  Meta server get list unsuccessful"


def main():
    if len(sys.argv) < 3:
        print("Invocation : <executable> <config_file> <command> <item_id>")
        exit(-1)
    config_path = sys.argv[1]
    command = sys.argv[2]
    item_id = sys.argv[3]

    # Make socket
    servPort = getRecServerPort(config_path)
    sock = getRecServerSocket(servPort)
    # Wrap in a protocol

    res_ = sock.ping()
    print(res_)

    req = ItemRequest()
    req.item_id = item_id
    print(req)
    rec_ = fetchRecByItem(sock, req)

    print(rec_)

if __name__ == "__main__":
    main()