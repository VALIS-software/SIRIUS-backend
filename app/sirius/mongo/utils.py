def doc_generator(collection, id_stream, batch_size=100000, **kwargs):
    batch_ids = []
    for one_id in id_stream:
        batch_ids.append(one_id)
        if len(batch_ids) >= batch_size:
            mongo_filter = {"_id": {"$in": batch_ids}}
            for d in collection.find(mongo_filter, **kwargs):
                yield d
            batch_ids = []
    else:
        if len(batch_ids) > 0:
            mongo_filter = {"_id": {"$in": batch_ids}}
            for d in collection.find(mongo_filter, **kwargs):
                yield d
