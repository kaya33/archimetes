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
    URL = "http://www.baixing.com/recapi/getAdByKw?ENABLE_PROFILING=1"
    kwdata = WebDataSource("post", URL).fetch_data(body,headers)
    return kwdata.json()



if __name__ == "__main__":

    data = {"num": 3,"city": "shanghai","category": "wupinhuishou","tag": "二手_相机","days": 400}
    response = fetchKwData(data)
