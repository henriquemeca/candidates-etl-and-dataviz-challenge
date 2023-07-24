import psycopg2

connection = psycopg2.connect(
    dbname="main",
    user="admin",
    password="admin",
    host="localhost",  # or wherever your database is hosted
)
