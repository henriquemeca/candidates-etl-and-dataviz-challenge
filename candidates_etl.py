from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType, IntegerType, StringType

from postgres_conector import PostgresConnector
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
        for field_name, field_types in self.schema.items():
            candidates_df = candidates_df.withColumn(
                field_name, col(field_name).cast(field_types)
            )
        return candidates_df

    def load(self, candidates_df: DataFrame) -> None:
        postgres_connector = PostgresConnector()

        candidates_df.write.jdbc(
            url=f"jdbc:postgresql://localhost/{postgres_connector.database}",
            table=self.table,
            mode=self.mode,
            properties={
                "user": postgres_connector.user,
                "password": postgres_connector.password,
            },
        )

    def execute(self):
        with spark_session(app_name="candidates_etl") as spark:
            candidates_df = self.transform(self.read(spark=spark))
            self.load(candidates_df)


if __name__ == "__main__":
    CandidatesETL().execute()
