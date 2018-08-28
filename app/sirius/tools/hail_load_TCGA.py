#!/usr/bin/env python

import os
import subprocess
from sirius.tools.rebuild_mongo_database import TCGA_URL
from sirius.parsers import TCGA_XMLParser, TCGA_MAFParser, TCGA_CNVParser

def write_hail_TCGA_patient_annotation_file(hail_folder, download_folder):
    current_dir = os.getcwd()
    # XML for patient info
    os.chdir(os.path.join(download_folder, 'BCRXML'))
    xml_files = []
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.xml'):
                xml_files.append(os.path.join(root, f))
    xml_files.sort()
    # transform parsed patientdata to annotation columns
    column_transform = {
        'patient_id': (lambda p: p['patient_id']),
        'patient_uuid': (lambda p: p['bcr_patient_uuid'].lower()),
        'patient_barcode': (lambda p: p['bcr_patient_barcode']),
        'age': (lambda p: -float(p['days_to_birth'])/365.0 if p.get('days_to_birth', None) else -1.0),
        'gender': (lambda p: p['gender']),
        'biosample': (lambda p: p.get('tumor_tissue_site', 'None')),
        'vital_status': (lambda p: p['vital_status']),
        'days_to_death': (lambda p: int(p['days_to_death']) if p.get('days_to_death', None) else -1),
        'histological_type': (lambda p: p.get('histological_type', 'None')),
        'drugs': (lambda p: p.get('drugs')),
        'disease_code' : (lambda p: p['disease_code'])
    }
    # start parsing
    all_patient_annotations = []
    print(f"Parsing {len(xml_files)} patient xml files")
    for f in xml_files:
        parser = TCGA_XMLParser(f, verbose=True)
        parser.parse()
        all_patient_annotations.append(tuple(trans(parser.patientdata) for trans in column_transform.values()))
    os.chdir(current_dir)
    # write as file
    filename = "patient_annotations.txt"
    fullpath = os.path.join(hail_folder, filename)
    with open(fullpath, "w") as afile:
        # write header line
        afile.write('\t'.join(column_transform.keys()) + '\n')
        # write data in lines
        for d in all_patient_annotations:
            afile.write('\t'.join(map(str, d)) + '\n')
    print(f"patient annotation file saved as {fullpath}")

def check_download(download_folder):
    current_dir = os.getcwd()
    os.chdir(download_folder)
    subprocess.check_call('wget -nc '+TCGA_URL, shell=True)
    filename = os.path.basename(TCGA_URL)
    if os.path.isdir('BCRXML'):
        print("Folder exists, skip decompressing")
    else:
        print(f"Decompressing {filename}")
        subprocess.check_call(f"tar zxf {filename} --skip-old-files", shell=True)
    os.chdir(current_dir)

def find_tcga_folder():
    subpath = 'gene_data_tmp/TCGA'
    if not os.path.isdir(subpath):
        os.makedirs(subpath)
    return subpath

def create_hail_tmp_folder():
    subpath = 'hail_data_tmp'
    if not os.path.isdir(subpath):
        os.mkdir(subpath)
    return subpath

def main():
    hail_folder = create_hail_tmp_folder()
    download_folder = find_tcga_folder()
    check_download(download_folder)
    write_hail_TCGA_patient_annotation_file(hail_folder, download_folder)

if __name__ == '__main__':
    main()
