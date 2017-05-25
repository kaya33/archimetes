#!/usr/bin/env python
#-*- coding: utf-8 -*-

import requests
import json

class WebDataSource(object):

    def __init__(self,type ,url):
        self.type = type
        self.url = url

    def fetch_data(self, body=None,headers=None):
        body = body
        try:
            r = requests.request(self.type, url=self.url, data=body, headers=headers, timeout=5)
            return r
        except Exception as e:
            print(e)

def fetchKwData(data,headers=None):
    if headers is None:
        headers = {
            'cache-control': "no-cache",
            'postman-token': "82f99b3a-2fcb-527e-2f13-1b2e5c3303bc"
        }
    body = json.dumps(data, ensure_ascii=False)
    print body
    # URL = "http://www.baixing.com/recapi/getAdByKw?ENABLE_PROFILING=1"
    URL = "http://www.baixing.com/recapi/getAdByKw"
    print URL
    kwdata = WebDataSource("post", URL).fetch_data(body,headers)
    print kwdata.json()
    return kwdata.json()



if __name__ == "__main__":

    data = {"category": "zhengzu", "city": "shanghai", "cut": 1000, "weight": [0.0299, 0.0838, 0.0359, 0.0598], "tag": "11000元_750元_100平米_60平米", "days": 270, "num": 100}
    response = fetchKwData(data)
    print response
