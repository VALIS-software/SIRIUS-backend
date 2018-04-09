#!/usr/bin/env python

import os, subprocess
import json
import requests
from sirius.parsers.BEDParser import BEDParser_ENCODE
from sirius.mongo.upload import update_insert_many
from sirius.mongo import GenomeNodes, InfoNodes, Edges

ENCODEURL = 'https://www.encodeproject.org'
HEADERS = {'accept': 'application/json'}
SEARCHURL = b'https://www.encodeproject.org/search/?type=Annotation&encyclopedia_version=4&files.file_type=bed+bed3%2B&assembly=hg19&organism.scientific_name=Homo+sapiens&limit=all'


def request_search():
    response = requests.get(SEARCHURL, headers=HEADERS)
    response_json_dict = response.json()
    with open('search_results.json','w') as outfile:
        json.dump(response_json_dict, outfile, indent=2)
    return response_json_dict

def download_parse_upload_data(response_json_dict):
    for data_dict in response_json_dict['@graph'][:10]:
        accession = data_dict['accession']
        description = data_dict['description']
        biosample = data_dict['biosample_term_name']
        targets = []
        if 'targets' in data_dict:
            for d in data_dict['targets']:
                targets.append(d['label'])
        if not os.path.isdir(accession):
            os.mkdir(accession)
        os.chdir(accession)
        file_info = download_annotation_bed(accession)
        filename = file_info['filename']
        metadata = {
            'accession': accession,
            'description': description,
            'biosample': biosample,
            'targets': targets,
            'assembly': file_info['assembly'],
            'sourceurl': file_info['sourceurl']
        }
        parse_upload_bed(filename, metadata)
        os.chdir('..')

def download_annotation_bed(accession):
    response = requests.get(f'{ENCODEURL}/annotations/{accession}', headers=HEADERS)
    response_json_dict = response.json()
    with open('annotations.json','w') as outfile:
        json.dump(response_json_dict, outfile, indent=2)
    bed_urls = []
    for file_dict in response_json_dict['files']:
        if file_dict['file_format'] == 'bed':
            bed_urls.append(file_dict['href'])
    assert len(bed_urls) == 1, f'Expecting 1 bed file, found {len(bed_urls)} on page'
    url = bed_urls[0]
    print(f'Found file {url}, downloading...')
    sourceurl = ENCODEURL + url
    filename = download_decompress_gz(sourceurl)
    print("\nDownload finished")
    return {'filename': filename, 'sourceurl': sourceurl, 'assembly': response_json_dict['assembly'][0]}


def download_decompress_gz(fileurl):
    filename = os.path.basename(fileurl)
    name, ext = os.path.splitext(filename)
    assert ext == '.gz', 'File should have .gz extension'
    if not os.path.isfile(name):
        subprocess.check_call('wget ' + fileurl, shell=True)
        subprocess.check_call('gzip -d ' + filename, shell=True)
    return name

def parse_upload_bed(filename, metadata):
    parser = BEDParser_ENCODE(filename, verbose=True)
    parser.parse()
    parser.metadata.update(metadata)
    genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
    print(f'parsing {filename} results in {len(genome_nodes)} GenomeNodes, {len(info_nodes)} InfoNodes, {len(edges)} Edges')
    #json.dump({'genome_nodes': genome_nodes, 'info_nodes': info_nodes, 'edges': edges}, open('mongo_nodes.json','w'), indent=2)
    print("Uploading to MongoDB")
    update_insert_many(GenomeNodes, genome_nodes)
    update_insert_many(InfoNodes, info_nodes)
    update_insert_many(Edges, edges)

def insert_encode_dataSource(metadata):
    from sirius.realdata.constants import DATA_SOURCE_ENCODE
    ds = DATA_SOURCE_ENCODE
    InfoNodes.insert_one({'_id': 'I'+ds, 'type': 'dataSource', 'name': ds, 'source': ds})

def main():
    search_dict = request_search()
    download_parse_upload_data(search_dict)
    insert_encode_dataSource({'searchUrl': SEARCHURL})

if __name__ == '__main__':
    main()
