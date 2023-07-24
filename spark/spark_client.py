from contextlib import contextmanager

from pyspark.sql import SparkSession


@contextmanager
def spark_session(app_name: str = "default_spark_app"):
    spark = (
        SparkSession.builder.appName(app_name)
        .master("local")
        .config("spark.driver.extraClassPath", "./spark/jars/postgresql-42.5.4.jar")
        .getOrCreate()
    )

    try:
        yield spark
    finally:
        spark.stop()
