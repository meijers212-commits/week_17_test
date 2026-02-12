import os
import mysql.connector

sql_host = os.getenv("SQL_HOST")
sql_user = os.getenv("SQL_USER")
sql_password = os.getenv("SQL_PASSWORD")
sql_db_name = os.getenv("SQL_DB_NAME")


mydb = mysql.connector.connect(
    host=sql_host,
    user=sql_user,
    password=sql_password
)

mycursor = mydb.cursor()


mycursor.execute(f"CREATE DATABASE IF NOT EXISTS {sql_db_name}")
mydb.commit()

print("✅ Database created")

mycursor.close()
mydb.close()


conn = mysql.connector.connect(
    host=sql_host,
    user=sql_user,
    password=sql_password,
    database=sql_db_name
)

cursor = conn.cursor()

print("✅ Connected to database")
