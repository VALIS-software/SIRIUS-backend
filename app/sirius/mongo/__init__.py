import os
from pymongo import MongoClient


uname, pwd = 'sirius', 'valis'
authenticationDatabase = 'testdb'

server_address = 'localhost'


client = MongoClient(server_address, username=uname, password=pwd, authSource=authenticationDatabase, connect=False)

# for test use, sirius is authorized to write here
testdb = client.testdb

# for main database, sirius can only read
# switching between these two databases allow us to 'hot-swap' the database during updates
db = client.database
#db = client.database1
GenomeNodes = db.GenomeNodes
InfoNodes = db.InfoNodes
Edges = db.Edges

