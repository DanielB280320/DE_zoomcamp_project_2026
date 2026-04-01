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

#Environment variables
path_local_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
gcs_credentials = os.environ.get('GCS_CREDENTIALS')
gcp_project_id = os.environ.get('PROJECT_ID')
gcs_bucket = os.environ.get('BUCKET_NAME')
bq_dataset = os.environ.get('DATASET_NAME')

conf = SparkConf() \
    .setMaster('local[*]') \
    .setAppName('project_pipeline') \
    .set("spark.driver.memory", "4g") \
    .set("spark.executor.memory", "4g") \
    .set("spark.jars", ",".join([
        "/opt/airflow/gcs_hadoop_conn/gcs-connector-hadoop3-2.2.5.jar",
        "/opt/airflow/gcs_hadoop_conn/spark-bigquery-with-dependencies_2.13-0.44.0.jar"
    ])) \
    .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
    .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", f"{path_local_home}/{gcs_credentials}")

#context
sc = SparkContext(conf=conf)

hadoop_conf = sc._jsc.hadoopConfiguration()

hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", f"{path_local_home}/{gcs_credentials}")
hadoop_conf.set("fs.gs.auth.service.account.enable", "true")

#Creating Spark session
spark = SparkSession.builder \
        .config(conf= sc.getConf()) \
        .getOrCreate()

bucket = gcs_bucket
spark.conf.set("temporaryGcsBucket", bucket)

print('Reading data from Google Cloud Storage...')
df_housing_gcs = \
    spark.read \
    .option('header', 'true') \
    .parquet(f'gs://{gcs_bucket}/weekly_housing_market_data_most_recent.parquet')
    

bq_path = f'{gcp_project_id}.{bq_dataset}.market_housing_data_consolidated'

#Repartitioning the data before writing to bq
df_housing_gcs = df_housing_gcs.repartition(10)

print('Writing data to Bigquery...')

df_housing_gcs \
    .write.format('bigquery') \
    .mode('overwrite') \
    .option('parentProject', gcp_project_id) \
    .option('partitionField', 'PERIOD_BEGIN') \
    .option('partitionType', 'MONTH') \
    .option('clusteredFields', 'REGION_NAME') \
    .save(bq_path)

print(f'Table {bq_path} was succesfully created')

spark.stop()
