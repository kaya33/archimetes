#!/usr/bin/env python
# -*- coding:utf-8 -*-

__author__ = 'xujiang@baixing.com'

# 作为开发环境的标准配置

configs = {
    'server': {
        'port': 9090,
        'thread_num': 100,
        'thriftMaxOpenMillisecond': 100,  # 设置连接的超时时间
        'thriftMaxProcessMillisecond': 30000,  # 设置存取数据的超时时间
        'framedTransportMaxLength': 102410241024        # 设置最大的数据帧长度
    } ,

    'user_profile':{
        'kw_size':4   #  user how many keywords to get post
    } ,
    'kafka': {
        'host': ['bj2-kafka01:9092', 'bj2-kafka02:9093', 'bj2-kafka03:9094'],
        'topic': 'eventlog',
        'group_id': 'test',
        'timeout': 10
    },

    'kafka_online_tag': {
        "host": ["bj2-kafka01:9092", "bj2-kafka02:9093", "bj2-kafka03:9094"],
        "group_id": "read_traffic",
        "timeout": 1000,
        "topic": "eventlog",
        "consume_num": 3,
        "mysql_api": "http://www.baixing.com/recapi/getAdInfoById?adId={}"
    },

    'mongo': {
        "dev_read_uri": "mongodb://127.0.0.1:27017",
        "dev_write_uri": "mongodb://127.0.0.1:27017",
        "prod_read_uri": "mongodb://127.0.0.1:27017",
        "prod_write_uri": "mongodb://127.0.0.1:27017"
    },

    'redis': {
        "host": "localhost",
        "port": 6379
    },

    'bloom_filter': {
        'capacity': 3000,
        'error_rate': 0.01,
        'rebuild_time': 86400
    }
}
