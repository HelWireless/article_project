# -*- coding:utf-8 -*-
"""
Author:hel
License: Apache Licence
File: article_update.py
Time: 2019/12/10
Version: 1.0
@Function:
"""

import os
import sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(BASE_DIR))

from reco_sys.offline import SparkSessionBase
from pyspark.ml.feature import BucketedRandomProjectionLSH
from datetime import datetime, timedelta
import pyspark.sql.functions as F
from pyspark.sql.functions import udf
from pyspark.sql.types import *
import gc
from reco_sys.setting.default import *
from pyspark.ml.linalg import Vectors
import logging
import codecs

# 停用词
global stopwords_list
stopwords_list = [i.strip() for i in codecs.open(STOP_WORDS_PATH).readlines()]

# 日志
logger = logging.getLogger("offline")


class UpdateArticle(SparkSessionBase):
    """
    更新文章画像
    """
    SPARK_APP_NAME = "updateArticle"
    ENABLE_HIVE_SUPPORT = True
    SPARK_EXECUTOR_CORES = 4
    SPARK_EXECUTOR_INSTANCES = 4

    def __init__(self):
        self.spark = self._create_spark_session()

        self.cv_path = CV_PATH
        self.idf_path = IDF_PATH

    def get_cv_model(self):
        # 词语与词频统计
        from pyspark.ml.feature import CountVectorizerModel
        cv_model = CountVectorizerModel.load(self.cv_path)
        return cv_model

    def get_idf_model(self):
        from pyspark.ml.feature import IDFModel
        idf_model = IDFModel.load(self.idf_path)
        return idf_model

    @staticmethod
    def compute_keywords_tfidf_topk(words_df, cv_model, idf_model):
        """保存tfidf值高的20个关键词
        :param spark:
        :param words_df:
        :return:
        """
        cv_result = cv_model.transform(words_df)
        tfidf_result = idf_model.transform(cv_result)

        # 取TOP-N的TFIDF值高的结果
        def func(partition):
            try:
                TOPK = 20
                for row in partition:
                    _ = list(zip(row.idfFeatures.indices, row.idfFeatures.values))
                    _ = sorted(_, key=lambda x: x[1], reverse=True)
                    result = _[:TOPK]

                    for word_index, tfidf in result:
                        yield row.article_id, row.channel_name, int(word_index), round(float(tfidf),
                                                                                       4), row.article_time

            except Exception as e:
                logger.info("{}, INFO: the TOP-N func has ERROR is {}".format(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))

        _keywordsByTFIDF = tfidf_result.rdd.mapPartitions(func).toDF(
                ["article_id", "channel_name", "index", "tfidf", "article_time"])
        logger.info("{}, INFO: the compute keywords tf-idf topK is complete".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        return _keywordsByTFIDF

    def merge_article_data(self):
        """
        合并业务中增量更新的文章数据
        :return:
        """
        # 获取文章相关数据, 指定过去一天整点到整点的更新数据
        # 如：26日：1：00~2：30，2：30~5：00，左闭右开
        try:
            self.spark.sql("use hbase_to_hive")
            _today = datetime.today().replace(minute=30, second=0, microsecond=0)
            start = datetime.strftime(_today+timedelta(days=0, hours=-2, minutes=0), "%Y-%m-%d %H:%M:%S")
            end = datetime.strftime(_today, "%Y-%m-%d %H:%M:%S")

            # 合并后保留：article_id、channel_name、title、title、content
            # +--------------------+-------------------------------------+------------+-------------------------------------+
            # | article_id | content      | channel_name  | title | channel_tags | article_describe | source | source_tags |
            # +--------------------+-------------------------------------+------------+-------------------------------------+
            # | 010e71     | 华为在武汉光谷国际网球... | IT之家 | 华为xyz...| [{"id": 14,"name":"xxx"| null | null |  null |

            basic_sql = """
                           select m_id as article_id , context as content, platform as channel_name, title,
                           our_tags as channel_tags, descr as article_describe, source, source_tags, time as article_time 
                           from news_info_update
                           where time >= '{}' and time < '{}'
                        """.format(start, end)

            logger.info("{}, INFO: this update time is from {}  to {},".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), start, end))

            channel_basic_content = self.spark.sql(basic_sql)

            @udf("string", StringType)
            def strip_null_udf(text):
                if text:
                    processed_text = text.replace('\n', '').replace('\r', '')
                else:
                    processed_text = "unknow"
                return processed_text

            channel_basic_content.fillna("unknow")
            channel_basic_content = channel_basic_content.withColumn('content',
                                                                     strip_null_udf(channel_basic_content.content))
            channel_basic_content = channel_basic_content.withColumn('article_describe', strip_null_udf(
                    channel_basic_content.article_describe))

            # 利用concat_ws方法，将多列数据合并为一个长文本内容（频道，标题以及内容合并）
            self.spark.sql("use article")
            sentence_df = channel_basic_content.select("article_id", "channel_name", "article_describe", "channel_tags",
                                                       "title", "content", "source", "source_tags", \
                                                       F.concat_ws(
                                                               ",",
                                                               channel_basic_content.source_tags,
                                                               channel_basic_content.channel_name,
                                                               channel_basic_content.title,
                                                               channel_basic_content.content
                                                               ).alias("sentence"),
                                                       "article_time"
                                                       )
            del channel_basic_content
            gc.collect()

            sentence_df = sentence_df.dropDuplicates()
            sentence_df.write.insertInto("article_data")
            logger.info("{}, INFO: the merge article data is complete".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        except Exception as e:
            sentence_df = None
            logger.info("{}, INFO: the merge article function has ERROR is {}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))
        return sentence_df

    # 分词函数
    @staticmethod
    def segmentation(partition):
        import jieba.posseg as pseg
        import re

        # 分词
        def cut_sentence(sentence):
            """对切割之后的词语进行过滤，去除停用词，保留名词，英文和自定义词库中的词，长度大于2的词"""
            # eg:[pair('今天', 't'), pair('有', 'd'), pair('雾', 'n'), pair('霾', 'g')]
            seg_list = pseg.lcut(sentence)
            seg_list = [i for i in seg_list if i.flag not in stopwords_list]
            filtered_words_list = []
            for seg in seg_list:
                if len(seg.word) <= 1:
                    continue
                elif seg.flag == "eng":
                    if len(seg.word) <= 2:
                        continue
                    else:
                        filtered_words_list.append(seg.word)
                elif seg.flag.startswith("n"):
                    filtered_words_list.append(seg.word)
                elif seg.flag in ["x", "eng"]:  # 是自定一个词语或者是英文单词
                    filtered_words_list.append(seg.word)
            return filtered_words_list

        for row in partition:
            sentence = re.sub("<.*?>", "", row.sentence)  # 替换掉标签数据
            words = cut_sentence(sentence)
            yield row.article_id, row.channel_name, words, row.article_time

    def generate_article_label(self, sentence_df):
        """
        生成文章标签  tfidf, textrank
        :param sentence_df: 增量的文章内容
        :return:
        """
        try:
            # 进行分词
            words_df = sentence_df.rdd.mapPartitions(self.segmentation).toDF(
                    ["article_id", "channel_name", "words", "article_time"])
            cv_model = self.get_cv_model()
            idf_model = self.get_idf_model()

            # 计算tfidf的值，并取前20个词
            _keywordsByTFIDF = UpdateArticle.compute_keywords_tfidf_topk(words_df, cv_model, idf_model)

            # 取出idf的关键词索引
            keywordsIndex = self.spark.sql("select keyword, index idx from idf_keywords_values")

            # 两表关联，把index变成词
            keywordsByTFIDF = _keywordsByTFIDF.join(keywordsIndex, keywordsIndex.idx == _keywordsByTFIDF.index).select(
                    ["article_id", "channel_name", "keyword", "tfidf", "article_time"])

            # 将当前的数据追加写入到hive表中
            keywordsByTFIDF.write.insertInto("tfidf_keywords_values")

            # 节省资源
            del cv_model
            del idf_model
            del words_df
            del _keywordsByTFIDF
            gc.collect()

            logger.info("{}, INFO: generate article label is complete".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        except Exception as e:
            logger.info("{}, INFO: generate article label has ERROR is {}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))

        return keywordsByTFIDF

    def get_article_profile(self, sentence_df, keywordsByTFIDF):
        """
        文章画像主题词建立
        :param idf: 所有词的idf值
        :param textrank: 每个文章的textrank值
        :return: 返回建立号增量文章画像
        """
        try:
            # 处理文章的空tags
            sentence_df.registerTempTable("temparticle1")
            source_tags = self.spark.sql(
                    "select article_id as article_id2, channel_name as channel_name2, source_tags, article_time from temparticle1")

            keywordsByTFIDF.registerTempTable("temparticle2")
            merge_keywords = self.spark.sql(
                    "select article_id, min(channel_name) channel_name, collect_list(keyword) keywords, collect_list(tfidf) weights from temparticle2 group by article_id")

            # 将source_tags加入到合并表
            keywords_info = merge_keywords.join(source_tags,
                                                merge_keywords.article_id == source_tags.article_id2).select(
                    ["article_id", "channel_name", "keywords", "weights", "source_tags", "article_time"])

            # 将unknown值更换成tags函数
            @udf("string", StringType())
            def change_unknow_udf(keywords, source_tags):
                if source_tags == "unknow":
                    source_tags = keywords
                elif source_tags == None:
                    source_tags = keywords
                elif len(source_tags) <= 3:
                    source_tags = keywords
                return source_tags

            keywords_info = keywords_info.withColumn("topics",
                                                     change_unknow_udf(keywords_info.keywords,
                                                                       keywords_info.source_tags))

            # 将关键词和权重合并成字典
            def _func(row):
                if row.topics is None:
                    row.topics = row.keywords
                arry_topics = row.topics.replace("[", "").replace("]", "").replace("\"", "").replace(" ", "").split(",")
                return row.article_id, row.channel_name, dict(
                        zip(row.keywords, row.weights)), arry_topics, row.article_time

            article_profile = keywords_info.rdd.map(_func).toDF(
                    ["article_id", "channel_name", "keyword", "topics", "article_time1"])

            Article_profile = article_profile.join(source_tags, article_profile.article_id == source_tags.article_id2,
                                                   "inner") \
                .select(["article_id", "channel_name", "keyword", "topics", "article_time"])

            # 将结果追加到article_profile中
            Article_profile.write.insertInto("article_profile")

            # 节省资源
            del sentence_df
            del keywordsByTFIDF
            del source_tags
            del merge_keywords
            del keywords_info
            del article_profile
            gc.collect()
            logger.info("{}, INFO: get article profile is complete".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        except Exception as e:
            logger.info("{}, INFO: get article profile has ERROR is {}".format(
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'), e))

        return Article_profile

    def compute_article_similar(self, articleProfile):
        """
        计算增量文章与历史文章的相似度 word2vec
        :return:
        """
        from pyspark.ml.linalg import Vectors
        def avg(row):
            """
            :param row: 取出每篇文章每个此地向量
            :return: 每篇文章的平均向量
            """
            x = 0
            for v in row.vectors:
                x += v
            #  将平均向量作为article的向量
            return row.article_id, row.channel_name, x/len(row.vectors), row.article_time

        def _array_to_article_vector(row):
            """
            :param row: 将数据类型转换成向量
            :return:
            """
            return row.article_id, Vectors.dense(row.articlevector), row.article_time

        def _array_to_word_vector(row):
            """
            :param row: 将数据类型转换成向量
            :return:
            """
            return row.word, Vectors.dense(row.vector)

        def toArray(row):
            return row.article_id, row.channel_name, [float(i) for i in
                                                      row.articlevector.toArray()], row.article_time

        # 需要限制的时间
        _yester = datetime.today().replace(minute=0, second=0, microsecond=0)
        limit_train_date = datetime.strftime(_yester+timedelta(days=-30, hours=0, minutes=0), "%Y-%m-%d %H:%M:%S")
        limit_test_date = datetime.strftime(_yester+timedelta(days=0, hours=-2, minutes=0), "%Y-%m-%d %H:%M:%S")


        # 计算向量
        articleProfile.registerTempTable("tempTable")
        Incremental_article_sql = "select article_id, channel_name, keywords, weights, article_time from tempTable LATERAL VIEW explode(keyword) AS keywords,weights "
        articleKeywordsWeights = self.spark.sql(Incremental_article_sql)


        vectors = self.spark.sql("select * from article.word_vector").repartition(400)
        vectors_vec = vectors.rdd.map(_array_to_word_vector).toDF(["word", "vector"])
        articleKeywordsWeightsAndVectors = articleKeywordsWeights.join(vectors_vec,
                                                                       vectors_vec.word == articleKeywordsWeights.keywords,
                                                                       "inner")
        articleKeywordVectors = articleKeywordsWeightsAndVectors.rdd.map(
                lambda r: (r.article_id, r.channel_name, r.keywords, r.weights*r.vector, r.article_time)).toDF(
                ["article_id", "channel_name", "keywords", "weightingVector", "article_time"])
        print("文章向量计算完成")

        articleKeywordVectors.registerTempTable("articleKeywordVectors")
        articleVector = self.spark.sql(
                "select article_id, min(channel_name) channel_name, collect_set(weightingVector) vectors, min(article_time) article_time from articleKeywordVectors group by article_id").rdd.map(
                avg).toDF(["article_id", "channel_name", "articlevector", "article_time"])
        print("articleVector 完成")

        # 写入数据库
        ArticleVector = articleVector.rdd.map(toArray).toDF(
                ['article_id', 'channel_name', 'articlevector', 'article_time'])
        ArticleVector.write.insertInto("article_vector")

        print("ArticleVector 完成")
        logger.info("{}, INFO: article vector write complete".format(
                datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

        # 得到历史数据,转换成固定格式使用LSH进行求相似
        test = self.spark.sql(
                "select * from article_vector where article_time >= '{}'".format(limit_test_date))
        test = test.rdd.map(_array_to_article_vector).toDF(['article_id', 'articlevector', 'article_time'])
        # print("test 完成")
        train = self.spark.sql(
                "select * from article_vector where article_time >= '{}'".format(limit_train_date))
        logger.info(
                "{}, INFO: the compute article train sql is: select * from article_vector where article_time >= {}".format(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'), limit_train_date))
        train = train.rdd.map(_array_to_article_vector).toDF(['article_id', 'articlevector', 'article_time'])

        # 使用LSH 做文章相似查询
        brp = BucketedRandomProjectionLSH(inputCol='articlevector', outputCol='hashes', seed=125,
                                          bucketLength=1.0)
        model = brp.fit(train)
        print("brp训练完成")

        similar = model.approxSimilarityJoin(test, train, 380, distCol='EuclideanDistance')
        print("相似结果获取完成")


        def save_redis(partition):
            """
            :param partition:
            :return: 将相似文章结果写入到redis中
            """
            import redis
            r = redis.StrictRedis(**REDIS_CONFIG)
            # article_similar redies data structure: {article_id:{similar_article_id : sim_score}}
            for row in partition:
                if row.datasetA.article_id == row.datasetB.article_id or row.EuclideanDistance == 0:
                    pass
                else:
                    r.zadd(str(row.datasetA.article_id),
                           {str(row.datasetB.article_id): "%0.4f"%row.EuclideanDistance})

        # 保存结果到redis
        similar.foreachPartition(save_redis)
        logger.info(
                "{}, INFO: the compute article similar  is complete".format(
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')))

# if __name__ == '__main__':
#
#     ua = UpdateArticle()
#     sentence_df = ua.merge_article_data()
#     print("完成文章合并")
#     try:
#         if sentence_df.take(1):
#             idf = ua.generate_article_label(sentence_df)
#             print("完成文章idf计算")
#             article_profile = ua.get_article_profile(sentence_df, idf)
#             print("完成文章画像")
#             ua.compute_article_similar(article_profile)
#             print("完成文章相似")
#         else:
#             logger.info("{}, INFO: article this hours no update because no article data update".format(
#                     datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
#     except Exception as e:
#         logging.info("{}, INFO: article update have error".format(
#                 datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
