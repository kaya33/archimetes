#!/usr/bin/env python
# -*-coding:utf-8-*-

__author__ = 'xujiang@baixing.com'

import os

import sys
if sys.version > '3':
    import queue
else:
    import Queue as queue
import threading
import contextlib
import datetime
import time

from threading import Thread

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
    transport.setTimeout(30000)
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
        print e
        log.error("Exception while connecting to block server, check if server is running on port: {}".format(port))
        transport.close()
        exit(1)

    return client

def fetchRecByItem(sock, req):
    try:
        begin = datetime.datetime.now()
        resp = sock.fetchRecByItem(req)
        end = datetime.datetime.now()
        print(resp)
        print "the Thread sleep  %s sec\n" % ( end - begin)
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
        begin = datetime.datetime.now()
        resp = sock.fetchRecByUser(req)
        end = datetime.datetime.now()
        print(resp)
        print "the rec by user cost time %s sec\n" % (end - begin)
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

def fetchRecByMult(sock, req):
    try:
        print req
        resp = sock.fetchRecByMult(req)
        print(resp)
    except Exception as e:
        print e
        log.error("ERROR while calling fetchRecByMult")
        log.error(e)
        print("ERROR,{}".format(e))
    if resp.status == responseType.OK:

        #print "Deletion of block successful"
        log.info("OK")
    else:
        #print "Deletion of block not successful"
        log.error("ERROR")


#!/usr/bin/env python
# -*- coding:utf-8 -*-
# File Name    : threadpool.py
# Author       : hexm
# Mail         : xiaoming.unix@gmail.com
# Created Time : 2017-03-23 20:03



StopEvent = object()  # 终止线程信号

class ThreadPool(object):
    """
    1、解决线程重用问题，当前线程执行完任务后，不杀掉，放到空闲线程列表，继续执行下个任务
    2、根据任务量开启线程，如果设置10个线程，只有2个任务，最多只会开启两个线程
    3、如果有500个任务，任务执行非常快，2个线程就能完成，如果设置开启10个线程，
        只会开启两个线程
    """

    def __init__(self, max_num, max_task_num = None):
        if max_task_num:
            self.q = queue.Queue(max_task_num)  # 指定任务最大数,默认为None,不限定
        else:
            self.q = queue.Queue()
        self.max_num = max_num  # 最多多少线程
        self.cancel = False  # 执行完所有任务，终止线程信号
        self.terminal = False  # 无论执行完毕与否，都终止所有线程
        self.generate_list = []  # 已创建多少线程
        self.free_list = []  # 空闲多少线程

    def run(self, func, args, callback=None):
        """
        线程池执行一个任务
        :param func: 任务函数
        :param args: 任务函数所需参数
        :param callback: 任务执行失败或成功后执行的回调函数，回调函数有两个参数1、任务函数执行状态；2、任务函数返回值
        :return: 如果线程池已经终止，则返回True否则None
        """
        if self.cancel:
            return
        # 没有空闲线程 并且已创建线程小于最大线程数才创建线程，
        if len(self.free_list) == 0 and len(self.generate_list) < self.max_num:
            self.generate_thread()  # 满足则创建线程，并将任务放进队列
        w = (func, args, callback,)
        # 函数，元组，函数 ，将这三个参数放在元组里面，当成一个整体放到队列里面
        self.q.put(w)  # 满足条件则创建线程，并把任务放队列里面

    def generate_thread(self):
        """
        创建一个线程
        """
        t = threading.Thread(target=self.call)  # 每一个线程被创建，执行call方法
        t.start()
        print "线程id：{}".format(t.ident)

    def call(self):
        """
        循环去获取任务函数并执行任务函数
        """
        current_thread = threading.currentThread()
        self.generate_list.append(current_thread)  # 每创建一个线程，将当前线程名加进已创建的线程列表

        event = self.q.get()  # 在队列中取任务, 没任务线程就阻塞，等待取到任务，线程继续向下执行
        while event != StopEvent:  # 是否满足终止线程

            func, arguments, callback = event  # 取出队列中一个任务
            try:
                result = func(*arguments)  # 执行函数，并将参数传进去
                success = True
            except Exception as e:
                success = False
                result = None

            if callback is not None:
                try:
                    callback(success, result)
                except Exception as e:
                    pass

            with self.worker_state(self.free_list, current_thread):  # 当前线程执行完任务，将当前线程置于空闲状态，
                #这个线程等待队列中下一个任务到来，如果没来，一直处于空闲, 如果到来，去任务
                if self.terminal:
                    event = StopEvent
                else:
                    event = self.q.get()   # 将当前任务加入到空闲列表后，如果有任务，取到，没有阻塞 取到后，移除当前线程
        else: # 满足终止线程，在创建的线程列表中移除当前线程
            self.generate_list.remove(current_thread)

    def close(self):
        """
        执行完所有的任务后，杀掉所有线程
        """
        self.cancel = True   # 标志设置为True
        full_size = len(self.generate_list) + 1  # 已生成线程个数, +1 针对python2.7
        while full_size:
            self.q.put(StopEvent)  #
            full_size -= 1

    def terminate(self):
        """
        无论是否还有任务，终止线程
        """
        self.terminal = True

        while self.generate_list:
            self.q.put(StopEvent)

        self.q.queue.clear()

    @contextlib.contextmanager
    def worker_state(self, state_list, worker_thread):
        """
        用于记录线程中正在等待的线程数
        """
        state_list.append(worker_thread)  # 将当前空闲线程加入空闲列表
        try:
            yield
        finally:
            state_list.remove(worker_thread)  # 取到任务后，将当前空闲线程从空闲线程里移除，

# 使用例子
if __name__ == "__main__":

    pool = ThreadPool(50)  # 创建pool对象，最多创建5个线程

    def callback(status, result):
        pass
    if len(sys.argv) < 2:
        log.error("Invocation : <executable> <config_file> <command> <item_id/user_id>")
        exit(-1)
    command = sys.argv[1]
    id = sys.argv[2]
    size = int(sys.argv[3])

    # Make socket

    # Wrap in a protocol
    if command == 'fetchRecByItem':
        req = ItemRequest()
        req.ad_id = id
        req.size = size

        servPort = getRecServerPort()
        for _ in range(1000):
            print _
            sock = getRecServerSocket(servPort)
            # fetchRecByItem(sock, req)
            pool.run(fetchRecByItem, (sock, req,), callback=None) # 将action函数，及action的参数，callback函数传给run()方法
            time.sleep(0.03)
            #time.sleep(0.05)
        pool.close()

    elif command == 'fetchRecByUser':
        req = UserRequest()
        req.user_id = id
        req.city_name = 'shanghai'
        req.first_category = 'fang'
        req.second_category = 'zhengzu'
        req.size = size
        servPort = getRecServerPort()
        sock = getRecServerSocket(servPort)
        rec_ = fetchRecByUser(sock, req)

    elif command == 'fetchRecByMult':
        req = MultRequest()
        req.user_id = id
        req.ad_id = sys.argv[4]
        req.city_name = 'shanghai'
        req.first_cat = 'fang'
        req.second_cat = 'zhengzu'
        servPort = getRecServerPort()
        sock = getRecServerSocket(servPort)
        print req
        rec_ = fetchRecByMult(sock, req)