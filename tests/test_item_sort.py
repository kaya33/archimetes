# -*- coding: utf-8 -*-

from archimedes.core import combine_sort

def test_sample_sort():
    x = [{'ad_id': 192168000, 'sim': 0.2}, {'ad_id': 192168002, 'sim': 0.3}]
    y = [{'ad_id': 192168001, 'sim': 0.5}, {'ad_id': 192168002, 'sim': 0.4}]
    print(combine_sort(x, y))

test_sample_sort()