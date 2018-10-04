def update_insert_many(dbCollection, nodes, update=True):
    if not nodes: return
    print(f"Uploading {dbCollection.name:12s}", end='', flush=True)
    prefix = dbCollection.name[0]
    all_ids = []
    for node in nodes:
        # we require all nodes have _id field start with prefix
        assert node['_id'][0] == prefix
        all_ids.append(node['_id'])
    all_ids_need_update = set()
    if update == True:
        # query the database in batches to find existing document with id
        batch_size = 100000
        for i_batch in range(int(len(all_ids) / batch_size)+1):
            batch_ids = all_ids[i_batch*batch_size:(i_batch+1)*batch_size]
            all_ids_need_update.update(result['_id'] for result in dbCollection.find({'_id': {'$in': batch_ids}}, projection=['_id']))
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
            dbCollection.insert_many(insert_nodes, ordered=False)
        except Exception as bwe:
            print('Error: ', bwe.details)
    for node in update_nodes:
        filt = {'_id': node.pop('_id')}
        update = {'$addToSet': {'source': node.pop('source')}}
        # take out the info key to treat later
        info_dict = node.pop('info')
        # overwrite the rest of the node
        update['$set'] = node
        # for info, we overwrite the single value, but merge the arrays
        for key, value in info_dict.items():
            update_key = 'info.'+key
            if isinstance(value, list):
                if len(value) == 1:
                    update['$addToSet'][update_key] = value[0]
                else:
                    update['$addToSet'][update_key] = {'$each': value}
            else:
                update['$set'][update_key] = value
        try:
            dbCollection.update_one(filt, update, upsert=True)
        except Exception as bwe:
            print('Error: ', bwe.details)
    print(f" finished. Updated: {len(update_nodes):10d} | Inserted: {len(insert_nodes):10d}" )


def update_skip_insert(dbCollection, nodes):
    if not nodes: return
    print(f"Uploading {dbCollection.name:12s}", end='', flush=True)
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
        all_ids_need_update.update(result['_id'] for result in dbCollection.find({'_id': {'$in': batch_ids}}, projection=['_id']))
    for node in nodes:
        if node['_id'] not in all_ids_need_update: continue
        filt = {'_id': node.pop('_id')}
        update = {'$addToSet': {'source': node.pop('source')}}
        # take out the info key to treat later
        info_dict = node.pop('info')
        # overwrite the rest of the node
        update['$set'] = node
        # for info, we overwrite the single value, but merge the arrays
        for key, value in info_dict.items():
            update_key = 'info.'+key
            if isinstance(value, list):
                if len(value) == 1:
                    update['$addToSet'][update_key] = value[0]
                elif len(value) > 1:
                    update['$addToSet'][update_key] = {'$each': value}
            else:
                update['$set'][update_key] = value
        if not update['$set']:
            update.pop('$set')
        try:
            dbCollection.update_one(filt, update, upsert=True)
        except Exception as bwe:
            print('Error: ', bwe.details)
    print(f" finished. Updated: {len(all_ids_need_update):10d}" )
