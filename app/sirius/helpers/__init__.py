from google.cloud import storage

class StorageBuckets(dict):
    """ cached gcloud storage buckets """
    storage_client = storage.Client()
    def __missing__(self, key):
        self[key] = self.storage_client.get_bucket(key)
        return self[key]

storage_buckets = StorageBuckets()

class KeyDict(dict):
    def __missing__(self, key):
        return key
