import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType, StringType

from postgres_params import PostgresParams
from spark.spark_client import spark_session


class CandidatesETL:
    table = "candidates"
    mode = "overwrite"

    schema = {
        "First Name": StringType(),
        "Last Name": StringType(),
        "Email": StringType(),
        "Application Date": DateType(),
        "Country": StringType(),
        "YOE": IntegerType(),
        "Seniority": StringType(),
        "Technology": StringType(),
        "Code Challenge Score": IntegerType(),
        "Technical Interview Score": IntegerType(),
    }

    def read(self, spark: SparkSession) -> DataFrame:
        return spark.read.csv(path="./data/candidates.csv", sep=";", header=True)

    def transform(self, candidates_df: DataFrame) -> DataFrame:
        for field_name, field_type in self.schema.items():
            candidates_df = candidates_df.withColumn(
                field_name, col(field_name).cast(field_type)
            )
        return candidates_df

    def load(self, candidates_df: DataFrame) -> None:
        candidates_df.write.jdbc(
            url=f"jdbc:postgresql://localhost/{PostgresParams.database_name}",
            table=self.table,
            mode=self.mode,
            properties={
                "user": PostgresParams.user,
                "password": PostgresParams.password,
            },
        )

    def execute(self) -> None:
        with spark_session("candidates-etl") as spark:
            candidates_df = self.transform(self.read(spark))
            self.load(candidates_df)


if __name__ == "__main__":
    CandidatesETL().execute()
