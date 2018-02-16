import os
from pymongo import MongoClient

if 'MONGO_SERVICE_HOST' in os.environ:
    client = MongoClient('mongodb://mongo:27017', connect=False)
else:
    client = MongoClient('35.197.96.144', 27017, connect=False)

db = client.database
GenomeNodes = db.GenomeNodes
