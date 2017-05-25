#!/usr/bin/env python
#-*- coding: utf-8 -*-

__author__ = 'xujiang@baixing.com'

from collections import Counter


def sample_sort(x):
    """
    :param x: [{},{}]
    :return: 
    """
    result_list = []
    seen = dict()
    for v in x:
        if v['rec_id'] not in seen.keys():
            seen[v['rec_id']] = float(v['sim'])
            result_list.append(v)
        else:
            seen[v['rec_id']] = v['sim'] + seen[v['rec_id']]
            result_list.append({'rec_id':v['rec_id'],'sim':seen[v['rec_id']]})
    return sorted(result_list, key=lambda k: k['sim'], reverse = True)


def sample_sort1(x):
    """
    用户画像结果的排序
    :param x: 
    :return: 
    """
    seen = dict()
    for v in x:
        seen.setdefault(str(v['rec_id']), 0)
        seen[str(v['rec_id'])] += v['sim']
    return sorted(seen.items(), key=lambda k: k[''], reverse = True)




