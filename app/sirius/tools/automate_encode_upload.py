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
        raise RuntimeError(f"Network error! Exit after {max_retries} retries.")

def request_search():
    result_fname = "sorted_response_graph.json"
    if os.path.isfile(result_fname):
        print(f"Loading search results from {result_fname}")
        with open(result_fname,'r') as jsonfile:
            sorted_response_graph = json.load(jsonfile)
    else:
        response = retry_get(SEARCHURL, headers=HEADERS)
        response_json_dict = response.json()
        response_graph = []
        for d in response_json_dict['@graph']:
            try:
                data_dict = {
                    'accession': d['accession'],
                    'description': d['description'],
                    'biosample': d['biosample_term_name'],
                    'targets': []
                }
                if 'targets' in d:
                    data_dict['targets'] = [td['label'] for td in d['targets']]
                response_graph.append(data_dict)
            except KeyError as err:
                print(f"Skipping accession {d['accession']} because of KeyError {err}")
        sorted_response_graph = sorted(response_graph, key=lambda d: d['accession'])
        with open(result_fname,'w') as outfile:
            json.dump(sorted_response_graph, outfile, indent=2)
            print(f"Search results saved to {result_fname}")
    return sorted_response_graph

def download_search_files(start=0, end=5):
    sorted_response_graph = request_search()
    n_total = len(sorted_response_graph)
    print(f"\n\n@@@ Search results have {n_total} datasets, downloading {start} to {end}.")
    print("-"*40)
    all_data_dicts = sorted_response_graph[start:end]
    for idata, data_dict in enumerate(all_data_dicts):
        data_idx = idata + start
        accession = data_dict['accession']
        print("\n\n@@@ Downloading %6d: accession %s" % (data_idx, accession))
        print("-"*40)
        afolder = '%05d_' % data_idx + accession
        if not os.path.exists(afolder):
            os.mkdir(afolder)
        os.chdir(afolder)
        file_info = download_annotation_bed(accession)
        filename = file_info['filename']
        metadata = data_dict.copy()
        metadata.update({
            'assembly': file_info['assembly'],
            'sourceurl': file_info['sourceurl'],
            'filename': filename
        })
        with open('metadata.json','w') as outfile:
            json.dump(metadata, outfile, indent=2)
        os.chdir('..')

def parse_upload_files(start=0, end=5):
    sorted_response_graph = request_search()
    n_total = len(sorted_response_graph)
    print(f"\n\n@@@ Search results have {n_total} datasets, parsing and uploading {start} to {end}.")
    print("-"*40)
    all_data_dicts = sorted_response_graph[start:end]
    for idata, data_dict in enumerate(all_data_dicts):
        data_idx = idata + start
        accession = data_dict['accession']
        print("\n\n@@@ Parsing %6d: accession %s" % (data_idx, accession))
        print("-"*40)
        afolder = '%05d_' % data_idx + accession
        if not os.path.exists(afolder):
            raise RuntimeError(f"{afolder} does not exist, call download first.")
        os.chdir(afolder)
        with open('metadata.json') as jsonfile:
            metadata = json.load(jsonfile)
        print("metadata.json loaded")
        parse_upload_bed(metadata)
        os.chdir('..')
    # we insert one InfoNode for dataSource
    insert_encode_dataSource()

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
    filename = download_gz(sourceurl)
    print("\nDownload finished")
    return {'filename': filename, 'sourceurl': sourceurl, 'assembly': response_json_dict['assembly'][0]}


def download_gz(fileurl):
    filename = os.path.basename(fileurl)
    if not os.path.isfile(filename):
        subprocess.check_call('wget ' + fileurl, shell=True)
    else:
        print(f"File {filename} already exists, skip downloading")
    return filename

def parse_upload_bed(metadata):
    filename = metadata['filename']
    parser = BEDParser_ENCODE(filename)
    parser.parse()
    parser.metadata.update(metadata)
    genome_nodes, info_nodes, edges = parser.get_mongo_nodes(liftover=True)
    print(f'parsing {filename} results in {len(genome_nodes)} GenomeNodes, {len(info_nodes)} InfoNodes, {len(edges)} Edges')
    print("Uploading to MongoDB")
    update_insert_many(GenomeNodes, genome_nodes, update=False)
    update_insert_many(InfoNodes, info_nodes, update=False)
    update_insert_many(Edges, edges, update=False)

def insert_encode_dataSource():
    from sirius.helpers.constants import DATA_SOURCE_ENCODE
    ds = DATA_SOURCE_ENCODE
    # prevent duplicate
    if not InfoNodes.find_one({'_id': 'I'+ds}):
        update_insert_many(InfoNodes, [{'_id': 'I'+ds, 'type': 'dataSource', 'name': ds, 'source': ds, 'info':{'searchURL': SEARCHURL}}])

def auto_parse_upload(start=0, end=5):
    download_search_files(start=start, end=end)
    parse_upload_files(start=start, end=end)

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument('-s', '--start', default=0, type=int, help="starting index of dataset")
    parser.add_argument('-e', '--end', default=5, type=int, help="ending index of dataset (exclusive)")
    args = parser.parse_args()
    auto_parse_upload(start=args.start, end=args.end)

if __name__ == '__main__':
    main()
