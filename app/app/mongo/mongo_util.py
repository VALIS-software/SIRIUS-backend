from ..main import app
import pymongo
import datetime

try:
    client = pymongo.MongoClient('mongodb://mongo:27017')
    db = client.test_database
except:
    print("Connecting to MongoDB database failed!")

@app.route("/healthcheck")
def test_mongo_get():
    all_data_str = '<br/>'.join(map(str, db.posts.find()))
    return "<p>"+all_data_str+"</p>"

@app.route("/insertdata")
def test_mongo_insert():
    post = {"author": "QYD",
            "text": "Hello!",
            "SNPs": ["rs6311", "rs3091244", "rs138055828", "rs148649884"],
            "date": datetime.datetime.utcnow()}
    db.posts.insert_one(post)
    return str(post)
