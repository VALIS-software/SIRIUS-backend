import os
import json
import tempfile
import hashlib
import uuid

from sirius.core.auth0 import get_user_name
from sirius.mongo import userdb, UserInfo
from sirius.mongo.upload import update_insert_many
from sirius.parsers import TxtParser_23andme, VCFParser, BEDParser

FILE_TYPE_PARSER = {
    '23andme': TxtParser_23andme,
    'vcf': VCFParser,
    'bed': BEDParser,
}

def get_user_id():
    uid = 'user_' + get_user_name()
    return uid

def upload_user_file(file_type, file_obj):
    """ Upload a file to the user's specific database """
    uid = get_user_id()
    # parse file and upload to a specific collection
    if file_type not in FILE_TYPE_PARSER:
        return f"Error: file type {file_type} not recognized"
    # save file_obj as a temporary file
    orig_filename = file_obj.filename
    ext_split = orig_filename.split(os.extsep, maxsplit=1)
    suffix = ext_split[1] if len(ext_split) == 2 else None
    tmp_filename = tempfile.mkstemp(suffix=suffix, prefix='ufile_')[1]
    file_obj.save(tmp_filename)
    # pick corresponding parser
    parser = FILE_TYPE_PARSER[file_type](tmp_filename, verbose=True)
    # parse and upload to userdb
    create_collection_user_file(parser, uid, orig_filename, file_type)
    # clean up the tmp file
    os.unlink(tmp_filename)
    print(f" @@@ User Operation | {uid} uploaded file {orig_filename}")
    return 'success'

def create_collection_user_file(parser, uid, orig_filename, file_type):
    # create a MongoDB collection in userdb to store genome_nodes from file
    file_id = 'Gfile_' + str(uuid.uuid4())
    collection = userdb.create_collection(file_id)
    # parse the file in chunks
    finished = False
    while not finished:
        finished = parser.parse_chunk()
        # use the orig filename as source
        parser.metadata['source'] = orig_filename
        genome_nodes, _, _ = parser.get_mongo_nodes()
        update_insert_many(collection, genome_nodes, update=False)
    # save file metadata in UserInfo
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
    return collection

def get_user_files_info():
    """ Get the list of files the user uploaded """
    uid = get_user_id()
    user_doc = UserInfo.find_one({'_id': uid})
    if user_doc:
        ret = user_doc.get('files', [])
    else:
        ret = []
    return ret

def delete_user_file(file_id):
    uid = get_user_id()
    user_doc = UserInfo.find_one({'_id': uid})
    # delete one file_info obj with matching file_id from the user_doc
    file_info_list = user_doc['files']
    doc_idx = None
    for i, file_info in enumerate(file_info_list):
        if file_info['fileID'] == file_id:
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
    # delete the file collection from userdb
    userdb.drop_collection(file_id)
    return 'success'