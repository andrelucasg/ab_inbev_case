from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
from azure.storage.blob import BlobServiceClient
from io import BytesIO
from delta import *
from delta.tables import *
from geopy.geocoders import Nominatim
from time import sleep
import io
import os
import pandas as pd
import pyspark
import requests

def azure_storage(action, data=None, df_schema=None, blob_prefix=None):

    def send_storage(data, df_schema, blob_prefix):

        if blob_prefix == "bronze":
           df_save_bronze = spark.createDataFrame(data, schema=df_schema)
           df_save_bronze.write.format("delta").mode("overwrite").save(azure_path_with_sas + blob_prefix)

        elif blob_prefix == "silver":
             data.write.format("delta").mode("overwrite").partitionBy("state").save(azure_path_with_sas + blob_prefix)

        elif blob_prefix == "gold":
             data.write.format("delta").mode("overwrite").save(azure_path_with_sas + blob_prefix)

    def read_from_storage(blob_prefix):

        if blob_prefix == "bronze":
           df_read_bronze = spark.read.format("delta").load(azure_path_with_sas + blob_prefix)
           return df_read_bronze

        elif blob_prefix == "silver":
             df_read_silver = spark.read.format("delta").load(azure_path_with_sas + blob_prefix)
             return df_read_silver

        elif blob_prefix == "gold":
            df_read_gold = spark.read.format("delta").load(azure_path_with_sas + blob_prefix)
            return df_read_gold

    global spark
    jars = [
        "/usr/local/spark/jars/delta-core.jar",
        "/usr/local/spark/jars/hadoop-azure.jar",
        "/usr/local/spark/jars/delta-storage.jar",
        "/usr/local/spark/jars/hadoop-azure-datalake.jar",
        "/usr/local/spark/jars/hadoop-client.jar",
        "/usr/local/spark/jars/hadoop-common.jar",
        "/usr/local/spark/jars/scala-library.jar",
        "/usr/local/spark/jars/azure-storage.jar",
        "/usr/local/spark/jars/jetty-util-ajax.jar",
        "/usr/local/spark/jars/jetty-util.jar"
    ]

    spark = SparkSession.builder.appName("OpenBreweryDB") \
        .config("spark.jars", ",".join(jars)) \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("fs.azure.account.key.storagename.dfs.core.windows.net", "key") \
        .config("fs.azure.sas.fixed.token.storagename.dfs.core.windows.net", "sas") \
        .getOrCreate()

    global container_client
    azure_storage_account_name = "storagename" 
    azure_container_name = "containername"

    azure_path = f"abfss://{azure_container_name}@{azure_storage_account_name}.dfs.core.windows.net/"
    azure_path_with_sas = f"{azure_path}"

    if action == "upload":
        send_storage(data, df_schema, blob_prefix)
    elif action == "read":
        return read_from_storage(blob_prefix)

def raw_def():
    url_api = "https://api.openbrewerydb.org/breweries"
    response = requests.get(url_api)
    data_api = response.json()
    return data_api

def bronze_def(ti):
    data = ti.xcom_pull(task_ids='raw_task', key='return_value')

    df_schema = StructType([
                    StructField("id", StringType(), True),
                    StructField("name", StringType(), True),
                    StructField("brewery_type", StringType(), True),
                    StructField("address_1", StringType(), True),
                    StructField("address_2", StringType(), True),
                    StructField("address_3", StringType(), True),
                    StructField("city", StringType(), True),
                    StructField("state_province", StringType(), True),
                    StructField("postal_code", StringType(), True),
                    StructField("country", StringType(), True),
                    StructField("longitude", StringType(), True),
                    StructField("latitude", StringType(), True),
                    StructField("phone", StringType(), True),
                    StructField("website_url", StringType(), True),
                    StructField("state", StringType(), True),
                    StructField("street", StringType(), True)])

    azure_storage(action="upload", data=data, df_schema=df_schema, blob_prefix="bronze")

def silver_def():
    df_bronze = azure_storage(action="read", blob_prefix="bronze")

    def get_new_info(lat, lon):

        geolocator = Nominatim(user_agent="your_app_name")
        
        try:
            location = geolocator.reverse(f"{lat}, {lon}").raw
            address = location.get('address', {})
            new_info = address.get('county')
        except Exception as e:
            print(f"latitude *{lat}* e longitude *{lon}* não possuem valores a serem coletados.")
            new_info = None
        
        sleep(1)
        return new_info
    
    def capitalize_first_letter(df, columns):
        for column in columns:
            if column in df.columns:
                df = df.withColumn(column, F.initcap(F.col(column)))
        return df
    
    get_new_info_udf = F.udf(get_new_info, StringType())
    df_silver_updated = df_bronze.withColumn("county", get_new_info_udf(F.col("latitude"), F.col("longitude")))

    columns_capitalize = ["brewery_type", "city", "state_province", "country", "state", "county"]
    df_silver_final = capitalize_first_letter(df_silver_updated, columns=columns_capitalize)

    df_schema = StructType([
                StructField("id", StringType(), True),
                StructField("name", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("address_1", StringType(), True),
                StructField("address_2", StringType(), True),
                StructField("address_3", StringType(), True),
                StructField("city", StringType(), True),
                StructField("state_province", StringType(), True),
                StructField("postal_code", StringType(), True),
                StructField("country", StringType(), True),
                StructField("longitude", StringType(), True),
                StructField("latitude", StringType(), True),
                StructField("phone", StringType(), True),
                StructField("website_url", StringType(), True),
                StructField("state", StringType(), True),
                StructField("street", StringType(), True),
                StructField("county", StringType(), True)])

    azure_storage(action="upload", data=df_silver_final, df_schema=df_schema, blob_prefix="silver")

def gold_def():
    df_silver = azure_storage(action="read", blob_prefix="silver")

    gold = df_silver \
    .groupBy("country", "state", "brewery_type") \
        .agg(
            F.count("id").alias("qtt_per_local")
        )

    df_schema = StructType([
                StructField("country", StringType(), True),
                StructField("state", StringType(), True),
                StructField("brewery_type", StringType(), True),
                StructField("qtt_per_local", IntegerType(), True)])

    azure_storage(action="upload", data=gold, df_schema=df_schema, blob_prefix="gold")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['andrelucasfpg@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 0,
}

with DAG("Case_AB_Inbev",
         start_date = datetime(2024, 1, 1),
         schedule_interval='*/10 * * * *',
         catchup = False) as dag:
    
    raw_task = PythonOperator(
        task_id = 'raw_task',
        python_callable=raw_def)
    
    bronze_task = PythonOperator(
        task_id='bronze_task',
        python_callable=bronze_def
    )

    silver_task = PythonOperator(
        task_id='silver_task',
        python_callable=silver_def
    )

    gold_task = PythonOperator(
        task_id='gold_task',
        python_callable=gold_def
    )

    raw_task >> bronze_task >> silver_task >> gold_task