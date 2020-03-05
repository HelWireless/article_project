#-*- coding:utf-8 -*-  
""" 
Author:hel 
License: Apache Licence 
File: logging.py 
Time: 2019/12/11
Version: 1.0
@Function:
"""
import logging
import logging.handlers
import os

logging_file_dir = '/home/data/logs/'
fmt = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
datefmt='%Y-%m-%d %H:%M:%S'

def create_logger():
    """
    设置日志
    :param app:
    :return:
    """

    # 离线处理更新打印日志
    trace_file_handler = logging.FileHandler(
        os.path.join(logging_file_dir, 'offline.log')
    )
    trace_file_handler.setFormatter(logging.Formatter(fmt))
    log_trace = logging.getLogger('offline')
    log_trace.addHandler(trace_file_handler)
    log_trace.setLevel(logging.INFO)

    # 离线处理更新打印日志
    trace_file_handler = logging.FileHandler(
            os.path.join(logging_file_dir, 'vec.log')
            )
    trace_file_handler.setFormatter(logging.Formatter(fmt))
    log_trace = logging.getLogger('vec')
    log_trace.addHandler(trace_file_handler)
    log_trace.setLevel(logging.INFO)