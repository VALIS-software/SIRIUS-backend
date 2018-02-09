# coding: utf-8
from pymongo import MongoClient
client = MongoClient('35.227.157.32', 27017)
db = client.test_database
data = posts.find_one()
print(data)
