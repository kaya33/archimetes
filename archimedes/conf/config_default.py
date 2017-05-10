#!/usr/bin/env python
# -*- coding:utf-8 -*-

__author__ = 'xujiang@baixing.com'

# 作为开发环境的标准配置

configs = {
    'server':{
       'port':9090
    } ,
    'kafka': {
        'host': ['bj2-kafka01:9092','bj2-kafka02:9093','bj2-kafka03:9094'],
        'group_id':'test',
        'timeout' : 10
    },
}
