import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType

# Initialize a Spark Session
spark = (
    SparkSession.builder.config(
        "spark.driver.extraClassPath", "./spark/jars/postgresql-42.5.4.jar"
    )
    .appName("candidates-csv")
    .master("local")
    .getOrCreate()
)

# Load CSV file into DataFrame
df = (
    spark.read.format("csv")
    .option("header", "true")
    .option("delimiter", ";")
    .load("./data/candidates.csv")
)
df.show()
df.printSchema()
schema = {
    "Application Date": DateType,
    "YOE": IntegerType,
    "Code Challenge Score": IntegerType,
    "Technical Interview Score": IntegerType,
}
for field_name, field_type in schema.items():
    df = df.withColumn(field_name, col(field_name).cast(field_type()))
df.show()
df.printSchema()


# Configure PostgreSQL connection
database = "main"
table = "candidates"
user = "admin"
password = os.getenv("POSTGRES_PASSWORD")
url = "localhost"

# Write the DataFrame to the PostgreSQL table
df.write.format("jdbc").option("url", f"jdbc:postgresql://localhost/{database}").mode(
    "overwrite"
).option("dbtable", table).option("user", user).option("password", password).save()
# .option(
#     "url", f"jdbc:postgresql://localhost/{database}"
# ).option("dbtable", table).option("user", user).option("password", password).option(
#     "driver", "org.postgresql.Driver"
# ).save()
