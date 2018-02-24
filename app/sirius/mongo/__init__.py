import os
from pymongo import MongoClient

try:
    uname, pwd = os.environ["MONGO_UNAME"], os.environ["MONGO_PWD"]
    authenticationDatabase = 'admin'
except:
    uname, pwd = 'sirius', 'valis'
    authenticationDatabase = 'testdb'

# we currently have one replica, more can be added here
client = MongoClient('mongo-0.mongo-service', username=uname, password=pwd, authSource=authenticationDatabase, connect=False)

# for test use, sirius is authorized to write here
testdb = client.testdb

# for main database, sirius can only read
db = client.database
GenomeNodes = db.GenomeNodes
InfoNodes = db.InfoNodes
EdgeNodes = db.EdgeNode
