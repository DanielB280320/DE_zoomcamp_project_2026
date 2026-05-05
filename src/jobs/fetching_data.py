import pandas as pd
import pyspark
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql import types
from pyspark.sql.functions import col
import urllib.request
import os

# pyspark.__file__

# Environment variables
path_local_home = os.environ.get('AIRFLOW_HOME', '/opt/airflow')
gcs_credentials = os.environ.get('GCS_CREDENTIALS')
gcs_bucket = os.environ.get('BUCKET_NAME')



# Spark configuration
def spark_config(path_local, cloud_credentials):
    conf = SparkConf() \
        .setMaster('local[*]') \
        .setAppName('project_pipeline') \
        .set("spark.driver.memory", "12g") \
        .set("spark.jars", "/opt/airflow/gcs_hadoop_conn/gcs-connector-hadoop3-2.2.5.jar") \
        .set("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .set("spark.hadoop.google.cloud.auth.service.account.json.keyfile", f"{path_local}/{cloud_credentials}")
    return conf

# Spark context
def spark_context(conf, path_local, cloud_credentials):
    sc = SparkContext(conf=conf)

    hadoop_conf = sc._jsc.hadoopConfiguration()

    hadoop_conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    hadoop_conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    hadoop_conf.set("fs.gs.auth.service.account.json.keyfile", f"{path_local}/{cloud_credentials}")
    hadoop_conf.set("fs.gs.auth.service.account.enable", "true")
    return sc

# Creating Spark session
def spark_session(sc):
    spark = SparkSession.builder \
            .config(conf= sc.getConf()) \
            .getOrCreate()
    return spark

# Fetching data from source server and storing it in a temporal directory:
def fetching_data():
    try:     
        print('Creating temporal directory to store the data')
        file_path = 'temp/weekly_housing_market_data_most_recent.tsv000.gz'
        os.makedirs(
            os.path.dirname(file_path), 
            exist_ok= True
        )

        # Getting historical data stored in aws s3
        url = 'https://redfin-public-data.s3.us-west-2.amazonaws.com/redfin_covid19/weekly_housing_market_data_most_recent.tsv000.gz'

        print(f'Extracting data from source {url}')
        urllib.request.urlretrieve(
            url, 'temp/weekly_housing_market_data_most_recent.tsv000.gz'
        )

        print(f'Data downloaded stored in path {file_path}')
    except Exception as e:
        print(f'Error fetching data: {e}')

# Initial Schema definition:
def schema_definition():
    schema_housing = types.StructType([
        types.StructField('PERIOD_BEGIN', types.TimestampType(), True),
        types.StructField('PERIOD_END', types.TimestampType(), True),
        types.StructField('REGION_TYPE', types.StringType(), True),
        types.StructField('REGION_TYPE_ID', types.IntegerType(), True),
        types.StructField('REGION_NAME', types.StringType(), True),
        types.StructField('REGION_ID', types.IntegerType(), True),
        types.StructField('DURATION', types.StringType(), True),
        types.StructField('ADJUSTED_AVERAGE_NEW_LISTINGS', types.DoubleType(), True),
        types.StructField('ADJUSTED_AVERAGE_NEW_LISTINGS_YOY', types.DoubleType(), True),
        types.StructField('AVERAGE_PENDING_SALES_LISTING_UPDATES', types.DoubleType(), True),
        types.StructField('AVERAGE_PENDING_SALES_LISTING_UPDATES_YOY', types.DoubleType(), True),
        types.StructField('OFF_MARKET_IN_TWO_WEEKS', types.IntegerType(), True),
        types.StructField('OFF_MARKET_IN_TWO_WEEKS_YOY', types.DoubleType(), True),
        types.StructField('ADJUSTED_AVERAGE_HOMES_SOLD', types.DoubleType(), True),
        types.StructField('ADJUSTED_AVERAGE_HOMES_SOLD_YOY', types.DoubleType(), True),
        types.StructField('MEDIAN_NEW_LISTING_PRICE', types.DoubleType(), True),
        types.StructField('MEDIAN_NEW_LISTING_PRICE_YOY', types.DoubleType(), True),
        types.StructField('MEDIAN_SALE_PRICE', types.DoubleType(), True),
        types.StructField('MEDIAN_SALE_PRICE_YOY', types.DoubleType(), True),
        types.StructField('MEDIAN_DAYS_TO_CLOSE', types.DoubleType(), True),
        types.StructField('MEDIAN_DAYS_TO_CLOSE_YOY', types.DoubleType(), True),
        types.StructField('MEDIAN_NEW_LISTING_PPSF', types.DoubleType(), True),
        types.StructField('MEDIAN_NEW_LISTING_PPSF_YOY', types.DoubleType(), True),
        types.StructField('ACTIVE_LISTINGS', types.IntegerType(), True),
        types.StructField('ACTIVE_LISTINGS_YOY', types.DoubleType(), True),
        types.StructField('MEDIAN_DAYS_ON_MARKET', types.DoubleType(), True),
        types.StructField('MEDIAN_DAYS_ON_MARKET_YOY', types.DoubleType(), True),
        types.StructField('PERCENT_ACTIVE_LISTINGS_WITH_PRICE_DROPS', types.DoubleType(), True),
        types.StructField('PERCENT_ACTIVE_LISTINGS_WITH_PRICE_DROPS_YOY', types.DoubleType(), True),
        types.StructField('AGE_OF_INVENTORY', types.DoubleType(), True),
        types.StructField('AGE_OF_INVENTORY_YOY', types.DoubleType(), True),
        types.StructField('WEEKS_OF_SUPPLY', types.DoubleType(), True),
        types.StructField('WEEKS_OF_SUPPLY_YOY', types.DoubleType(), True),
        types.StructField('MEDIAN_PENDING_SQFT', types.DoubleType(), True),
        types.StructField('MEDIAN_PENDING_SQFT_YOY', types.DoubleType(), True),
        types.StructField('AVERAGE_SALE_TO_LIST_RATIO', types.DoubleType(), True),
        types.StructField('AVERAGE_SALE_TO_LIST_RATIO_YOY', types.DoubleType(), True),
        types.StructField('MEDIAN_SALE_PPSF', types.DoubleType(), True),
        types.StructField('MEDIAN_SALE_PPSF_YOY', types.DoubleType(), True),
        types.StructField('LAST_UPDATED', types.TimestampType(), True),
    ])

    df_housing = \
        spark.read \
        .option('sep', '\t') \
        .option('header', 'true') \
        .schema(schema_housing) \
        .csv('temp/weekly_housing_market_data_most_recent.tsv000.gz')

    records_extracted = df_housing.count()

    print(f'Records extracted from source: {records_extracted}')

    return df_housing

# print(df_housing.rdd.getNumPartitions)

# Uploading data to GCS in parquet format:
def upload_to_gcs(df, bucket_name):
    print('Uploading data to Google Cloud Storage...')

    df \
        .repartition(10) \
        .write \
        .mode('overwrite') \
        .parquet(f'gs://{bucket_name}/weekly_housing_market_data_most_recent.parquet')

    print('Data was succesfully stored')

    spark.stop()

def main():
    conf = spark_config(path_local_home, gcs_credentials)
    sc = spark_context(conf, path_local_home, gcs_credentials)
    global spark
    spark = spark_session(sc)

    fetching_data()
    df_housing = schema_definition()
    upload_to_gcs(df_housing, gcs_bucket)

if __name__ == "__main__": 
    main()






