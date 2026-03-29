
import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.context import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.functions import col
from urllib import request
import os

pyspark.__file__

#configuration
gcs_credentials = '/home/daniel/de_zoomcamp_2026_project/gcs_credentials/credentials/service_account_creds.json'

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('project_pipeline') \
    .set("spark.driver.memory", "4g") \
    .set("spark.executor.memory", "8g") \
    .set("spark.jars", ",".join([
        "/home/daniel/de_zoomcamp_2026_project/gcs_hadoop_conn/gcs-connector-hadoop3-2.2.5.jar",
        "/home/daniel/de_zoomcamp_2026_project/gcs_hadoop_conn/spark-bigquery-with-dependencies_2.13-0.44.0.jar"
    ])) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", gcs_credentials)

#context
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", gcs_credentials)
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

#Creating Spark session
spark = SparkSession.builder \
        .config(conf= sc.getConf()) \
        .getOrCreate()

bucket = "de-zoomcamp-2026-project-bucket"
spark.conf.set("temporaryGcsBucket", bucket)

print('Reading data from Google Cloud Storage...')
df_housing_gcs = \
    spark.read \
    .option('header', 'true') \
    .parquet('gs://de-zoomcamp-2026-project-bucket/weekly_housing_market_data_most_recent.parquet')

#df_housing_gcs.printSchema()

#df_housing_gcs.show(truncate= False, n= 10)

#print(df_housing_gcs.rdd.getNumPartitions)

bq_path = 'project-0c3c5223-416f-4242-b0f.test_dataset.market_housing_data_consolidated'

#Repartitioning the data before writing to bq
df_housing_gcs = df_housing_gcs.repartition(10)

print('Writing data to Bigquery...')
df_housing_gcs \
    .write.format('bigquery') \
    .mode('overwrite') \
    .option('partitionField', 'PERIOD_BEGIN') \
    .option('partitionType', 'MONTH') \
    .option('clusteredFields', 'REGION_NAME') \
    .save(bq_path)

print(f'Table {bq_path} was succesfully created')
spark.stop()
