#!/usr/bin/env python

import os, subprocess, time
import json
import requests
from sirius.parsers.BEDParser import BEDParser_ENCODE
from sirius.mongo.upload import update_insert_many
from sirius.mongo import GenomeNodes, InfoNodes, Edges

ENCODEURL = 'https://www.encodeproject.org'
HEADERS = {'accept': 'application/json'}
SEARCHURL = 'https://www.encodeproject.org/search/?type=Annotation&encyclopedia_version=4&files.file_type=bed+bed3%2B&assembly=hg19&organism.scientific_name=Homo+sapiens&limit=all'

def retry_get(url, headers, max_retries=20, timeout=10):
    retry_on_exceptions = (
         requests.exceptions.Timeout,
         requests.exceptions.ConnectionError,
         requests.exceptions.HTTPError
    )
    for i in range(max_retries):
        try:
            result = requests.get(url, headers=headers)
            return result
        except retry_on_exceptions:
            time.sleep(timeout)
            print(f"Connection failed, retrying {i}th time ...")
            continue
    else:
        raise RuntimeError("Network error!")

def request_search():
    response = retry_get(SEARCHURL, headers=HEADERS)
    response_json_dict = response.json()
    with open('search_results.json','w') as outfile:
        json.dump(response_json_dict, outfile, indent=2)
    return response_json_dict

def download_parse_upload_data(response_json_dict, start=0, end=None):
    all_data_dicts = sorted(response_json_dict['@graph'], key=lambda d: d['accession'])
    n_total = len(all_data_dicts)
    print(f"\n\n@@@ Search results in {n_total} datasets, parsing {start} to {end} in this run.")
    print("-"*40)
    if end == None:
        end = n_total
    all_data_dicts = all_data_dicts[start:end]
    for idata, data_dict in enumerate(all_data_dicts):
        accession = data_dict['accession']
        print("\n\n@@@%6d: accession %s" % (idata+start, accession))
        print("-"*40)
        description = data_dict['description']
        biosample = data_dict['biosample_term_name']
        targets = []
        if 'targets' in data_dict:
            for d in data_dict['targets']:
                targets.append(d['label'])
        afolder = 'tmp_' + accession
        if not os.path.exists(afolder):
            os.mkdir(afolder)
        os.chdir(afolder)
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
        # we remove this file to save disk space
        os.remove(filename)
        os.chdir('..')

def download_annotation_bed(accession):
    response = retry_get(f'{ENCODEURL}/annotations/{accession}', headers=HEADERS)
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
    print("Uploading to MongoDB")
    update_insert_many(GenomeNodes, genome_nodes, update=False)
    update_insert_many(InfoNodes, info_nodes, update=False)
    update_insert_many(Edges, edges, update=False)

def insert_encode_dataSource(metadata):
    from sirius.realdata.constants import DATA_SOURCE_ENCODE
    ds = DATA_SOURCE_ENCODE
    InfoNodes.insert_one({'_id': 'I'+ds, 'type': 'dataSource', 'name': ds, 'source': ds})

def auto_parse_upload(start=0, end=None):
    search_dict = request_search()
    download_parse_upload_data(search_dict, start=start, end=end)
    insert_encode_dataSource({'searchUrl': SEARCHURL})

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', type=int, help="starting index of dataset")
    parser.add_argument('-e', '--end', type=int, help="ending index of dataset (exclusive)")
    args = parser.parse_args()
    auto_parse_upload(start=args.start, end=args.end)

if __name__ == '__main__':
    main()
