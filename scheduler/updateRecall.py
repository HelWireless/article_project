#-*- coding:utf-8 -*-  
""" 
Author:hel 
License: Apache Licence 
File: updateRecall.py 
Time: 2020/01/09
Version: 1.0
@Function:
"""
import os, sys
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath('__file__')))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, 'reco_sys'))
from reco_sys.offline.als_recall import UpdateRecall
import logging

logging.getLogger("als_recall")

def update_recall():
    """
    更新用户的召回集
    :return:
    """
    udp = UpdateRecall(50)
    udp.als_recall()
    udp.spark.stop()




if __name__=='__main__':
    update_recall()

