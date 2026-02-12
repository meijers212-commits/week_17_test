import os
from pymongo import MongoClient

db_name = os.getenv("MONGO_DB")
my_coll = os.getenv("MONGO_COLLECTION")
db_port = os.getenv("MONGO_PORT")
db_host = os.getenv("MONGO_HOST")


mongo_uri = f"mongodb://{db_host}:{db_port}"

client = MongoClient(mongo_uri)

mydb = client[db_name]
mycollection = mydb[my_coll]
