#!/usr/bin/env python

def update_insert_many(dbCollection, nodes):
    if not nodes: return
    prefix = dbCollection.name[0]
    all_ids = []
    for node in nodes:
        # we require all nodes have _id field start with prefix
        assert node['_id'][0] == prefix
        all_ids.append(node['_id'])
    all_ids_need_update = set()
    # query the database in batches to find existing document with id
    batch_size = 100000
    for i_batch in range(int(len(all_ids) / batch_size)+1):
        batch_ids = all_ids[i_batch*batch_size:(i_batch+1)*batch_size]
        ids_need_update = set([result['_id'] for result in dbCollection.find({'_id': {'$in': batch_ids}}, projection=['_id'])])
        all_ids_need_update |= ids_need_update
    insert_nodes, update_nodes = [], []
    for node in nodes:
        if node['_id'] in all_ids_need_update:
            update_nodes.append(node)
        else:
            node['source'] = [node['source']]
            insert_nodes.append(node)
            # later node with the same id will be put into update_nodes
            all_ids_need_update.add(node['_id'])
    if insert_nodes:
        try:
            dbCollection.insert_many(insert_nodes)
        except Exception as bwe:
            print(bwe.details)
    for node in update_nodes:
        filt = {'_id': node.pop('_id')}
        update = {'$push': {'source': node.pop('source')}}
        # merge the info key instead of overwrite
        if 'info' in node and isinstance(node['info'], dict):
            info_dict = node.pop('info')
            for key, value in info_dict.items():
                node['info.'+key] = value
        update['$set'] = node
        try:
            dbCollection.update_one(filt, update, upsert=True)
        except Exception as bwe:
            print(bwe.details)
    print("%s finished. Updated: %d  Inserted: %d" % (dbCollection.name, len(update_nodes), len(insert_nodes)))


