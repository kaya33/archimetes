#!/usr/bin/env python
# -*- coding:utf-8 -*-

__author__ = 'xujiang@baixing.com'

# 作为开发环境的标准配置

configs = {
    'server':{
       'port':9090,
        'thread_num':100,
        'thriftMaxOpenMillisecond':100,  # 设置连接的超时时间
        'thriftMaxProcessMillisecond':30000,  # 设置存取数据的超时时间
        'framedTransportMaxLength': 102410241024        # 设置最大的数据帧长度

    } ,
    'kafka': {
        'host': ['bj2-kafka01:9092','bj2-kafka02:9093','bj2-kafka03:9094'],
        'topic': 'eventlog',
        'group_id':'test',
        'timeout' : 10
    },
}
