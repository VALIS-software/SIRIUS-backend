# coding: utf-8
from pymongo import MongoClient
client = MongoClient('35.227.157.32', 27017)
db = client.test_database
post = {"author": "QYD", "Message": "Hello World", "Date": "2.9.2018"}
posts = db.posts
posts.insert_one(post)
print(db.collection_names(include_system_collections=False))
