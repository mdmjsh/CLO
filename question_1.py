#!/usr/bin/env python
# coding: utf-8

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql import SQLContext

# https://medium.com/@sivachaitanya/accessing-aws-s3-from-pyspark-standalone-cluster-6ef0580e3c08

spark_context = SparkContext()
# spark_context.setSystemProperty('com.amazonaws.services.s3.enableV4', 'true')

# hadoopConf = spark_context._jsc.hadoopConfiguration()
# hadoopConf.set('fs.s3a.awsAccessKeyId', 'WwF^nc6dplGM')
# hadoopConf.set('fs.s3a.awsSecretAccessKey', 'AKIAT6VGICDBXRWLOQMT')
# hadoopConf.set('fs.s3a.endpoint', 's3-ap-south-1.amazonaws.com')
# hadoopConf.set('com.amazonaws.services.s3a.enableV4', 'true')
# hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

BIG_TAXI = 's3a://chictaxi/chictaxi.csv'
SMALL_TAXI = 's3a://chictaxi/small.csv'
WEATHER = 's3a://chictaxi/weather.csv'

sql_context = SQLContext(sc)  # noqa


def get_data(sql_context, path=SMALL_TAXI):
    df = sql_context.read.csv(path, header='true', inferSchema='true')
    return (df, df.rdd)


taxi_df, taxi_rdd = get_data(sql_context)