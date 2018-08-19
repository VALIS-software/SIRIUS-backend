import os
import json
import tempfile
import hashlib

from sirius.core.auth0 import get_user_profile
from sirius.mongo import userdb, UserInfo
from sirius.mongo.upload import update_insert_many
from sirius.parsers.txt_parser import TxtParser_23andme

FILE_TYPE_PARSER = {
    '23andme': TxtParser_23andme
}

def get_user_id():
    user_profile = json.loads(get_user_profile())
    uid = 'user_' + user_profile['name']
    return uid

def get_file_hash(filename):
    hasher = hashlib.md5()
    BLOCKSIZE = 65536
    with open(filename, 'rb') as afile:
        buf = afile.read(BLOCKSIZE)
        while len(buf) > 0:
            hasher.update(buf)
            buf = afile.read(BLOCKSIZE)
    return hasher.hexdigest()

def upload_user_file(file_type, file_obj):
    """ Upload a file to the user's specific database """
    uid = get_user_id()
    # parse file and upload to a specific collection
    if file_type not in FILE_TYPE_PARSER:
        return f"Error: file type {file_type} not recognized"
    # save file_obj as a temporary file
    orig_filename = file_obj.filename
    tmp_filename = tempfile.mkstemp(prefix='ufile_')[1]
    file_obj.save(tmp_filename)
    # create a MongoDB collection in userdb to store genome_nodes from file
    file_hashstr = get_file_hash(tmp_filename)
    collection_name = 'Gfile_' + file_hashstr
    existing_collection_names = userdb.list_collection_names()
    # we only parse and upload new files that is not in the database
    if collection_name not in existing_collection_names:
        collection = userdb.create_collection(collection_name)
        parser = FILE_TYPE_PARSER[file_type](tmp_filename, verbose=True)
        finished = False
        while not finished:
            finished = parser.parse_chunk()
            genome_nodes, _, _ = parser.get_mongo_nodes()
            update_insert_many(collection, genome_nodes, update=False)
    else:
        collection = userdb.get_collection(collection_name)
    # clean up the tmp file
    os.unlink(tmp_filename)
    # save file metadata in UserInfo
    file_id = collection_name
    file_num_docs = collection.estimated_document_count()
    file_info = {
        'fileName': orig_filename,
        'fileType': file_type,
        'fileID': file_id,
        'numDocs': file_num_docs,
    }
    update_doc = {
        '$set': {
            '_id': uid,
        },
        '$push': {
            'files': file_info
        }
    }
    UserInfo.update_one({'_id': uid}, update_doc, upsert=True)
    print(f" @@@ User Operation | {uid} uploaded file {orig_filename}")
    return 'success'

def get_user_files_info():
    """ Get the list of files the user uploaded """
    uid = get_user_id()
    user_doc = UserInfo.find_one({'_id': uid})
    if user_doc:
        ret = user_doc.get('files', [])
    else:
        ret = []
    return ret

def delete_user_file(fileID):
    uid = get_user_id()
    user_doc = UserInfo.find_one({'_id': uid})
    # delete one file_info obj with matching fileID from the user_doc
    file_info_list = user_doc['files']
    doc_idx = None
    for i, file_info in enumerate(file_info_list):
        if file_info['fileID'] == fileID:
            doc_idx = i
            break
    else:
        return 'File Not Found'
    file_info_list.pop(doc_idx)
    # update the database
    update_doc = {
        '$set': {
            'files': file_info_list
        }
    }
    UserInfo.update_one({'_id': uid}, update_doc)
    return 'success'