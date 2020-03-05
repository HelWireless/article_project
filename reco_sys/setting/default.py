#-*- coding:utf-8 -*-  
""" 
Author:hel 
License: Apache Licence 
File: default.py 
Time: 2019/12/03
Version: 1.0
@Function:
"""

REDIS_CONFIG = {
                    "host"    : "r-uf6ey5mfqih7vpflmr.redis.rds.aliyuncs.com",
                    "port"    : 6379,
                    "password": "A6y5KVugiF",
                    "db"      : 2
                    }

CV_PATH = "hdfs://192.168.0.35:8020/models/CV.model"
IDF_PATH = "hdfs://192.168.0.35:8020/models/IDF.model"
STOP_WORDS_PATH = "/home/data/etc/stop_words.txt"
WORD2VEC_PATH = "hdfs://192.168.0.35:8020/models/w2v_5W_2020_01_02.model"


HBASE_CONFIG = {
        "size"     : 10, "host": '192.168.0.35', "port": 9090
        }