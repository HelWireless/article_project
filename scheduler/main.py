# -*- coding:utf-8 -*-
""" 
Author:hel 
License: Apache Licence 
File: main.py 
Time: 2019/12/03
Version: 1.0
@Function:
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath('__file__')))
sys.path.insert(0, os.path.join(BASE_DIR))
sys.path.insert(0, os.path.join(BASE_DIR, 'reco_sys'))

from scheduler.updateArticle import update_article_profile
from scheduler.updateRecall import update_recall
from scheduler.send_email import send_email, QQ_OPTIONS
from reco_sys.setting import logging as lg
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
import logging

# 创建各spark任务日志
lg.create_logger()

# 创建apscheduler日志
logging.basicConfig(level=logging.WARNING,
                    filename='/home/data/logs/apscheduler_task.log',
                    filemode='w',
                    format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')


# 监控邮件函数
def my_listener(event):
    if event.exception:
        SCRIP_NAME = "prod__update"
        send_email(QQ_OPTIONS, SCRIP_NAME, event.exception)
        print('任务出错了！！！！！！')
    else:
        print('任务照常运行...')


scheduler = BlockingScheduler()

# 添加定时更新任务更新文章,每隔1小时更新
scheduler.add_job(update_article_profile, trigger='cron', hour='7-22', minute='50', id='article_update')
scheduler.add_job(update_recall, trigger='cron', hour=4, minute='30', id='als_recall')
scheduler.add_listener(my_listener, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)
scheduler._logger = logging

scheduler.start()




