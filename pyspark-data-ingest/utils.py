import pathlib
import findspark

findspark.init()
from pyspark.sql import SparkSession

def manage_spark_session(app_name):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark