# -*- coding: utf-8 -*-
import os
import sys
import heapq
import json
#default_encoding = 'utf-8'
#if sys.getdefaultencoding() != default_encoding:
#    reload(sys)
#    sys.setdefaultencoding(default_encoding)

os.environ['SPARK_HOME'] = r'/usr/local/spark-default'
sys.path.append("/usr/local/spark-default/python")
sys.path.append("/usr/local/spark-default/python/lib/py4j-0.10.4-src.zip")
sys.path.append("/usr/lib64/python2.6/site-packages")

from pyspark import SparkContext, SparkConf

app_name = "spark_read_top_8_word"
master = "yarn"
conf = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=conf)


def get_path():

    all_path = dict()

    all_path['in'] = 'hdfs://mainhadoop/user/leiyishan/ad_break/000000_0'
    all_path['out'] = 'hdfs://mainhadoop/user/hauser/AI/output/ad_break'

    return all_path



def main():

    all_path = get_path()

    in_list_rdd = sc.textFile(all_path['word'])
    # print(ad_list_rdd.map(lambda a: json.loads(a.split('\t')[1])).first())

    (in_list_rdd.map(lambda a: a.split('{')[0], dict(sorted(json.loads('{' + a.split('{')[1]).items(),
                                                            key=lambda d: d[1], reverse=True)[:8]))
     .saveAsTextFile(all_path['out']))

    # (sc.parallelize(word_list_rdd)
    #       .cartesian(ad_list_rdd)
    #       .map(lambda (a, b): count_value(a, b))
    #       .filter(lambda x: x[1][1] > 1000)
    #       .groupByKey()
    #       .mapValues(list)
    #       .map(lambda (a, b): (a, [x[0] for x in heapq.nlargest(50, b, key=lambda x: x[1])]))
    #       .saveAsTextFile(all_path['out'])
    #       #.takeOrdered(10, key=lambda x: -x[1])
    #       # .reduce(lambda a, b: )
    #       #.first())
    #       )

if __name__ == '__main__':
    main()