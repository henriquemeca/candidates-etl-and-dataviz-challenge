import os

import psycopg2
from dotenv import load_dotenv


class PostgresConnector:
    def __init__(self) -> None:
        load_dotenv()
        self.user: str = os.getenv("POSTGRES_USER")
        self.password: str = os.getenv("POSTGRES_PASSWORD")
        self.database: str = os.getenv("POSTGRES_DB")
        self.host = "localhost"

    def get_connection(self):
        return psycopg2.connect(
            dbname=self.database,
            user=self.user,
            password=self.password,
            host=self.host,
        )
