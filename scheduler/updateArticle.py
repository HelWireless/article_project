# -*- coding:utf-8 -*-
""" 
Author:hel 
License: Apache Licence 
File: updateArticle.py
Time: 2019/12/03
Version: 1.0
@Function:
"""
import logging
from datetime import datetime
from reco_sys.offline.article_update import UpdateArticle

logging.getLogger("offline")


def update_article_profile():
    """
      更新文章画像
      :return:
      """
    ua = UpdateArticle()
    sentence_df = ua.merge_article_data()
    print("完成文章合并")
    try:
        if sentence_df.take(1):
            idf = ua.generate_article_label(sentence_df)
            print("完成文章idf计算")
            article_profile = ua.get_article_profile(sentence_df, idf)
            print("完成文章画像")
            ua.compute_article_similar(article_profile)
            ua.spark.stop()
        else:
            ua.spark.stop()
            logging.info("{}, INFO: article this hours no update because no article data update".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
    except Exception as e:
        ua.spark.stop()
        logging.info("{}, INFO: article merge have error".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))