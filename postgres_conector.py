import os
from contextlib import contextmanager

from dotenv import load_dotenv
from pandas import DataFrame
from psycopg2 import connect


class PostgresConnector:
    def __init__(self) -> None:
        load_dotenv()
        self.user: str = os.getenv("POSTGRES_USER")
        self.password: str = os.getenv("POSTGRES_PASSWORD")
        self.database: str = os.getenv("POSTGRES_DB")
        self.host = "localhost"

    def __get_connection(self):
        return connect(
            dbname=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
        )

    @contextmanager
    def __cursor_manager(self):
        "Create a connection and cursor to the database and garanties to close them"
        connection = self.__get_connection()
        cursor = connection.cursor()
        try:
            yield cursor
        finally:
            cursor.close()
            connection.close()

    def query_data(self, query: str) -> DataFrame:
        "Sends a query request to the configured postgres database and returns a pandas Dataframe with the data"

        with self.__cursor_manager() as cursor:
            cursor.execute(query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            return DataFrame(rows, columns=columns)
