# -*- coding:utf-8 -*-
"""
Author:hel
License: Apache Licence
File: als_recall.py
Time: 2020/01/09
Version: 1.0
@Function:
"""
import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))
from pyspark.ml.feature import StringIndexer
from pyspark.ml import Pipeline
from reco_sys.setting.default import HBASE_CONFIG
from reco_sys.offline import SparkSessionBase
from pyspark.ml.recommendation import ALS
import logging
import pyspark.sql.functions as F
from datetime import datetime, timedelta

logger = logging.getLogger("als_recall")


class UpdateRecall(SparkSessionBase):
    SPARK_APP_NAME = "updateRecall"
    ENABLE_HIVE_SUPPORT = True
    SPARK_EXECUTOR_MEMORY = "2g"
    SPARK_EXECUTOR_CORES = 4
    SPARK_EXECUTOR_INSTANCES = 3
    ENABLE_HIVE_SUPPORT = True
    BUFFER_MAX = 512
    MAX_EXECUTORS = 20
    MEMORY_OVERHEAD = 1024
    CACHE_SIZE = 1047483648

    def __init__(self, number):
        self.spark = self._create_spark_session()
        # 推荐相似数量
        self.N = number

    def als_recall(self):
        """
            als 模型召回
        """

        # 更换数据类型
        def change_types(row):
            return row.user_id, row.article_id.replace("\"", ""), int(row.clicked)

        # als召回数据跨度
        _today = datetime.today().replace(minute=0, second=0, microsecond=0)
        limit_date = datetime.strftime(_today+timedelta(days=-60, hours=0, minutes=0), "%Y-%m-%d")
        logging.info("{}, INFO: als recall data is from {}".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S'), limit_date))

        user_article_click = self.spark.sql(
                "SELECT user_id, articles as article_id, max(click_art) as clicked FROM behavior_data.dws_article_behavior "
                "where dt>'{}' GROUP BY articles,user_id".format(limit_date))
        user_article_click = user_article_click.rdd.map(change_types).toDF(['user_id', 'article_id', 'clicked'])

        # 用户和文章ID使用StringIndexer进行转换
        user_id_indexer = StringIndexer(inputCol='user_id', outputCol='als_user_id')
        article_id_indexer = StringIndexer(inputCol='article_id', outputCol='als_article_id')
        pip = Pipeline(stages=[user_id_indexer, article_id_indexer])
        pip_fit = pip.fit(user_article_click)
        als_user_article_click = pip_fit.transform(user_article_click)

        # 模型训练和推荐默认每个用户固定文章个数
        als = ALS(userCol='als_user_id', itemCol='als_article_id', ratingCol='clicked', checkpointInterval=1)
        model = als.fit(als_user_article_click)
        logging.info("{}, INFO: als model is completed".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        recall_res = model.recommendForAllUsers(self.N)

        # recall_res结果与原用户id和文章id映射回去
        refection_user = als_user_article_click.groupBy(['user_id']).max('als_user_id').withColumnRenamed(
                'max(als_user_id)', 'als_user_id')
        refection_article = als_user_article_click.groupBy(['article_id']).max('als_article_id').withColumnRenamed(
                'max(als_article_id)', 'als_article_id')

        # Join推荐结果与 refection_user映射关系表
        # +-------------+-----------------------+----------------------+
        # | als_user_id | recommendations       | user_id              |
        # +-------------+-----------------------+----------------------+
        # | 1           | [[163, 0.91328144]... | 3                    |
        # | 2           | [[145, 0.653115], ... | 1106476833370537984  |
        recall_res = recall_res.join(refection_user, on=['als_user_id'], how='left').select(
                ['als_user_id', 'recommendations', 'user_id'])

        # Join推荐结果与 refection_article映射关系表
        # +-------------+---------------------+-------------------+
        # | als_user_id | user_id             | als_article_id    |
        # +-------------+---------------------+-------------------+
        # | 8           | 2                   | [163, 0.91328144] |
        # | 8           | 2                   | [132, 0.91328144] |

        recall_res = recall_res.withColumn('als_article_id', F.explode('recommendations')).drop('recommendations')

        # +-------------+-----------+----------------+
        # | als_user_id |   user_id | als_article_id |
        # +-------------+-----------+----------------+
        # | 8           | 2         | 163            |
        # | 8           | 2         | 132            |
        def _article_id(row):
            return row.als_user_id, row.user_id, row.als_article_id[0]

        als_recall = recall_res.rdd.map(_article_id).toDF(['als_user_id', 'user_id', 'als_article_id'])
        als_recall = als_recall.join(refection_article, on=['als_article_id'], how='left').select(
                ['user_id', 'article_id'])
        # 得到每个用户ID 对应推荐文章
        # +---------------------+------------+
        # | user_id             | article_id |
        # +---------------------+------------+
        # | 1106476833370537984 | 44075      |
        # | 1                   | 44075      |

        # 将相似文章结果合并成array
        als_recall = als_recall.groupBy(['user_id']).agg(F.collect_list('article_id')).withColumnRenamed(
                'collect_list(article_id)', 'article_list')
        als_recall = als_recall.dropna()
        logging.info("{}, INFO: als recall is complete".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))


        # 将结果写入Hbase
        def save_offline_recall_hbase(partition):
            """离线模型召回结果存储
            """
            import happybase
            pool = happybase.ConnectionPool(**HBASE_CONFIG)
            for row in partition:
                with pool.connection() as conn:
                    # 过滤reco_article重复值
                    reco_res = list(set(row.article_list))
                    if reco_res:
                        table = conn.table('article_user_recommend_version')
                        # 将结果写入Hbase,als列族
                        table.put(row.user_id.encode(),
                                  {'als:als_ids'.encode(): str(reco_res).replace("\'", "\"").encode()})
                    conn.close()

        als_recall.foreachPartition(save_offline_recall_hbase)
        logging.info("{}, INFO: als recall data has wrote into Hbase".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

#
# if __name__ == '__main__':
#     ur = UpdateRecall(50)
#     ur.als_recall()
#     ur.spark.stop()
