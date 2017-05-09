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
            r = requests.request(self.type, url=self.url, data=body.encode('utf-8'),headers=headers, timeout=5)
            return r
        except Exception as e:
            print(e)




def fetchKwData(body,headers):
    URL = "http://www.leiyishan.baixing.cn/recapi/getAdByKw"

    kwdata = WebDataSource("post", URL).fetch_data(body,headers)
    return kwdata



if __name__ == "__main__":

    payload = {"num": 3,"city": "shanghai","category": "wupinhuishou","tag": "二手_相机","days": 400}
    body = json.dumps(payload, ensure_ascii=False)

    headers = {
        'cache-control': "no-cache",
        'postman-token': "82f99b3a-2fcb-527e-2f13-1b2e5c3303bc"
    }

    response = fetchKwData(body=body, headers=headers)

    print(response.text)