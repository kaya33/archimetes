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
            print self.type
            print self.url, body, headers
           
            #body = {"category": "zhengzu", "city": "shanghai", "cut": 1001, "weight": [0.0538, 0.0538, 0.0359, 0.0658], "tag": "精装修_铁岭_960元_1室", "days": 270, "num": 100} 
           # body = json.dumps(body, ensure_ascii=False)
            #r = requests.post("http://news.baixing.com/feed.php?type=list&cate=rec", data=body, headers=headers)
            r = requests.request(self.type, url=self.url, data=body, headers=headers, timeout=5)
            print 'r.json',r.json(), r.raw
            return r
        except Exception as e:
            print(e)

def fetchKwData(data,headers=None):
    if headers is None:
        headers = {
            'cache-control': "no-cache",
            'postman-token': "82f99b3a-2fcb-527e-2f13-1b2e5c3303bc"
        }
    body =data
    body = json.dumps(data, ensure_ascii=False)
    #print body
    #  URL = "http://www.baixing.com/recapi/getAdByKw?ENABLE_PROFILING=1"
    URL = "http://www.baixing.com/recapi/getAdByKw"
    print URL
    kwdata = WebDataSource("post", URL).fetch_data(body,headers)
    print kwdata
    return kwdata.json()



if __name__ == "__main__":
     data = {"category": "zhengzu", "city": "shanghai", "cut": 1000, "weight": [0.0538, 0.0538, 0.0359, 0.0658], "tag": "精装修_铁岭_960元_1室", "days": 270, "num": 100}

     #data = {"category": "zhengzu", "city": "shanghai", "cut": 1000, "weight": [0.0479, 0.0359, 0.0059, 0.0359], "tag": "50平米_960元_贸易城西_100平米", "days": 270, "num": 100}
     response = fetchKwData(data)
