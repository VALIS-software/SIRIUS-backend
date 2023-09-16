from google.cloud import storage

class StorageBuckets(dict):
    """ cached gcloud storage buckets """
    def __init__(self):
        self.storage_client = storage.Client()
        
    def __missing__(self, key):
        self[key] = self.storage_client.get_bucket(key)
        return self[key]

try:
    storage_buckets = StorageBuckets()
except:
    print("google cloud connection failed")
    storage_buckets = dict()

class KeyDict(dict):
    def __missing__(self, key):
        return key
