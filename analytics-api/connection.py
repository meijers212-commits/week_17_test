import os
import mysql.connector


sql_host = os.getenv("SQL_HOST")
sql_user = os.getenv("SQL_USER")
sql_password = os.getenv("SQL_PASSWORD")
sql_db_name = os.getenv("SQL_DB_NAME")

conn = mysql.connector.connect(
  host=sql_host,
  user=sql_user,
  password=sql_password,
  database=sql_db_name
)

cursor = conn.cursor()