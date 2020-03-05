#-*- coding:utf-8 -*-  
""" 
Author:hel 
License: Apache Licence 
File: __init__.py 
Time: 2019/12/03
Version: 1.0
@Function: basic config
"""

from pyspark import SparkConf
from pyspark.sql import SparkSession



class SparkSessionBase(object):

    SPARK_APP_NAME = None
    SPARK_URL = "yarn"

    SPARK_EXECUTOR_MEMORY = "2g"
    SPARK_EXECUTOR_CORES = 4
    SPARK_EXECUTOR_INSTANCES = 4

    DRIVER_MEMORY = '2g'
    ENABLE_HIVE_SUPPORT = True
    BUFFER_MAX = 1024
    MAX_EXECUTORS = 26
    MEMORY_OVERHEAD = 2480
    CACHE_SIZE = 2047483648
    MEMORY_FRACTION = 0.6
    SAOTRAGE_FRACTION = 0.35

    def _create_spark_session(self):
        """穿件初始化spark session"""
        conf = SparkConf()  # 创建spark config对象
        config = (
                ("spark.app.name", self.SPARK_APP_NAME),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
                ("spark.executor.memory", self.SPARK_EXECUTOR_MEMORY),  # 设置该app启动时占用的内存用量，默认2g
                ("spark.master", self.SPARK_URL),  # spark master的地址
                ("spark.executor.cores", self.SPARK_EXECUTOR_CORES),  # 设置spark executor使用的CPU核心数，默认是1核心
                ("spark.executor.instances", self.SPARK_EXECUTOR_INSTANCES),
                ("spark.kryoserializer.buffer.max", self.BUFFER_MAX),
                ('spark.driver.memory', self.DRIVER_MEMORY),
                ('spark.dynamicAllocation.maxExecutors', self.MAX_EXECUTORS),
                ('spark.executor.memoryOverhead', self.MEMORY_OVERHEAD),
                ('spark.dynamicAllocation.enabled', True),
                ('spark.sql.hive.filesourcePartitionFileCacheSize', self.CACHE_SIZE),
                ("spark.memory.fraction", self.MEMORY_FRACTION),
                ("spark.memory.storageFraction", self.SAOTRAGE_FRACTION),
                ("spark.port.maxRetries", "40"),
                ("spark.dynamicAllocation.maxExecutors", 5),  # 最多占用6个Executor
                # ("spark.dynamicAllocation.executorIdleTimeout", 60),  # executor闲置时间
                # ("spark.dynamicAllocation.cachedExecutorIdleTimeout", 60)  # cache闲置时间
                # ("spark.sql.broadcastTimeout", 600),
                # ("spark.sql.autoBroadcastJoinThreshold", "-1")
        )

        conf.setAll(config)

        # 利用config对象，创建spark session
        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()

    def _create_spark_hbase(self):
        conf = SparkConf()  # 创建spark config对象
        config = (
            ("spark.app.name", self.SPARK_APP_NAME),  # 设置启动的spark的app名称，没有提供，将随机产生一个名称
            ("spark.executor.memory", self.SPARK_EXECUTOR_MEMORY),  # 设置该app启动时占用的内存用量，默认2g
            ("spark.master", self.SPARK_URL),  # spark master的地址
            ("spark.executor.cores", self.SPARK_EXECUTOR_CORES),  # 设置spark executor使用的CPU核心数，默认是1核心
            ("spark.executor.instances", self.SPARK_EXECUTOR_INSTANCES),
            ("hbase.zookeeper.quorum", "192.168.0.35"),
            ("hbase.zookeeper.property.clientPort", "2181")
        )

        conf.setAll(config)

        # 利用config对象，创建spark session
        if self.ENABLE_HIVE_SUPPORT:
            return SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
        else:
            return SparkSession.builder.config(conf=conf).getOrCreate()