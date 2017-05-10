import hashlib
import threading
import queue
import requests
import datetime
import json
import time
import sys
import re
import subprocess

from gevent import monkey;monkey.patch_socket()
import gevent

SHARE_Q = queue.Queue()  #构造一个不限制大小的的队列
_WORKER_THREAD_NUM = 3   #设置线程个数

def request_post(url, data):
    try:
        return requests.post(url=url, data=data).text
    except Exception:
        return 'not ok'

def send_to_api(data_dict):
    m = hashlib.md5()
    #url = 'http://www.baixing.com/p/juheapi.php?action={0}&token={1}'
    url = 'http://xujiang.baixing.cn/p/recapi.php?action={0}&token={1}'
    if data_dict == []:
        return
    if len(data_dict) > 100:
        return
    action = 'updateREC'
    #body = str(data_dict)
    salt = 'xujiang'
    body = json.dumps(data_dict, ensure_ascii=False)
    m.update((action + body + salt).encode('utf-8'))
    token = m.hexdigest()
    try:
        post_request = requests.post(url=url.format(action, token), data=body.encode('utf-8'))
        print(post_request.text)
    except Exception as e:
        print(e)

def process_row(raw_data):
    tmp_data = dict()
    try:
        row = re.split('\t',raw_data)
        data = row[1].replace("'","\"").replace("u","")
        id, rec_name, id_type, value_list = row[0], 'itemCF', '1', json.loads(data)
        print(type(value_list))
        assert id != ''
    except Exception as e:
        print(e)
        return []
    tmp_data['rec_id'] = id
    tmp_data['rec_name'] = rec_name
    tmp_data['id_type'] = id_type
    tmp_data['ads'] = value_list
    return tmp_data

def read_hdfs_and_send(url):
    """
    Accept hdfs path to read the contents of the deposit mongo
    :param : 
    :return: 
    """
    if url:
        print(url)
        tmp_100_data = list()
        ge_100_list = list()
        # cat = subprocess.Popen(["hadoop", "fs", "-cat", url], stdout=subprocess.PIPE)
        # for line in cat.stdout:
        f = open(url, 'r')
        for line in f.readlines():
            raw_data = line.strip()
            tmp_process_row = process_row(raw_data)
            if len(tmp_process_row) > 0:
                tmp_100_data.append(tmp_process_row)

            if len(tmp_100_data) > 99:
                ge_100_list.append(gevent.spawn(send_to_api, tmp_100_data[:]))
                tmp_100_data = []

            if len(ge_100_list) > 2:
                gevent.joinall(ge_100_list)
                ge_100_list = list()

        if len(tmp_100_data) > 0:
            ge_100_list.append(gevent.spawn(send_to_api, tmp_100_data[:]))

        if len(ge_100_list) > 0:
            gevent.joinall(ge_100_list)

        f.close()



def main():
    if len(sys.argv) > 1:
        dt = sys.argv[1]
    else:
        today = datetime.date.today()
        dt = (today - datetime.timedelta(1)).strftime('%Y%m%d')
    p = subprocess.Popen('hadoop fs -ls /user/hauser/xujiang/rec/fang/recommend_step5/{0}/part*'.format(dt[2:]), shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = p.communicate()
    url_list = [x.split(' ')[-1] for x in output.decode('utf8').split('\n')[1:-1]]
    time.sleep(3)
    try:
        p.terminate()
    except Exception as e:
        print(e)

    for url in url_list:
        read_hdfs_and_send(url)


if __name__ == '__main__':
    read_hdfs_and_send("/Users/louis/Ideaprojects/recommender/jerusalem/resource/part-00029")

    #for i in range(1,100):
    #rec_dict['rec_id'] = rec_dict.get('rec_id');
    # send_to_api(rec_dict)
    # read_hdfs_and_send('/Users/louis/IdeaProjects/recommender/jerusalem/resource/part-00094')