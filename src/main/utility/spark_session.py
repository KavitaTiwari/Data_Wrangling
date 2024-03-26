from pyspark.sql import *


def spark_session():
    spark = SparkSession.builder.master("local[*]") \
        .appName("DataWranglingAssignment4") \
        .getOrCreate()
    return spark
