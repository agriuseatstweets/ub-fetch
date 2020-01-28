from pyspark.sql import SparkSession
from clize import run
import orjson
import os
import logging

def build_spark():
    keys = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

    CWD='/home/jupyter/work' # set in Docker image

    spark = SparkSession \
        .builder \
        .appName("Fetch UB Originals") \
        .config("spark.jars", f"{CWD}/gcs-connector-hadoop2-latest.jar,{CWD}/gcs-connector-hadoop2-latest.jar,{CWD}/spark-sql-kafka-0-10_2.11-2.4.4.jar,{CWD}/kafka-clients-2.4.0.jar") \
        .config("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem") \
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true") \
        .config("spark.hadoop.google.cloud.auth.service.account.json.keyfile", keys) \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "1g") \
        .getOrCreate()

    return spark


def write(month):
    drivers = os.getenv('KAFKA_BROKERS')
    topic = os.getenv('REHYDRATE_TOPIC')


    spark = build_spark()
    tweets = spark.read.parquet(f'gs://spain-tweets/ub-originals') \
                       .where(f'month = {month}')

    tweets \
        .rdd \
        .map(lambda x: x.asDict(recursive=True)) \
        .map(lambda tw: (str(tw['id']), orjson.dumps(tw))) \
        .mapValues(lambda x: x.decode('utf8')) \
        .toDF(['key', 'value']) \
        .write \
        .format('kafka') \
        .option("kafka.bootstrap.servers", drivers) \
        .option("topic", topic) \
        .save()

    logging.info('WROTE TO KAFKA!')

if __name__ == '__main__':
    run(write)
