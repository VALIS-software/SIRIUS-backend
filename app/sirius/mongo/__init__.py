import os
from pymongo import MongoClient

# allow user specify authorization
try:
    uname, pwd = os.environ["MONGO_UNAME"], os.environ["MONGO_PWD"]
    authenticationDatabase = 'admin'
except:
    uname, pwd = 'sirius', 'valis'
    authenticationDatabase = 'testdb'

if 'KUBERNETES_SERVICE_HOST' in os.environ:
    # we currently have one replica, more can be added here
    server_address = 'mongo-0.mongo-service'
elif 'MONGO_PORT' in os.environ:
    # This is used when we have container linkage like --link some-mongo:mongo when debugging inside docker containers
    server_address = 'mongo'
else:
    # use kubectl port-forward mongo-0 27017:27017 to forward port if dev locally
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
