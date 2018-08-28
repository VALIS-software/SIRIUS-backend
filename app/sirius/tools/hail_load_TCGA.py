#!/usr/bin/env python

import os, subprocess
from sirius.tools.rebuild_mongo_database import TCGA_URL
from sirius.parsers import TCGA_XMLParser, TCGA_MAFParser, TCGA_CNVParser


def write_hail_TCGA_annotation_files():
    # three subfolders have been prepared and we will parse them one by one
    # XML for patient info
    os.chdir('BCRXML')
    xml_files = []
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.xml'):
                xml_files.append(os.path.join(root, f))
    xml_files.sort()
    all_patient_annotations = []
    # this is used in MAF parser
    patient_barcode_tumor_site = dict()
    # these are used in CNV parser
    patient_uuid_tumor_site = dict()
    patient_uuid_barcode = dict()
    print(f"Parsing {len(xml_files)} patient xml files")
    for f in xml_files:
        parser = TCGA_XMLParser(f, verbose=True)
        parser.parse()
        p = parser.patientdata
        days_to_death = int(p['days_to_death']) if p['days_to_death'] else -1
        current_age = -int(p['days_to_birth'])/365.0 if p['days_to_birth'] else -1
        parsed = {
            'patient_id': p['patient_id'],
            'patient_uuid': p['bcr_patient_uuid'].lower(),
            'patient_barcode': p['bcr_patient_barcode'],
            'age': current_age,
            'gender': p['gender'],
            'biosample': p.get('tumor_tissue_site', 'None'),
            'vital_status': p['vital_status'],
            'days_to_death': days_to_death,
            'histological_type': p.get('histological_type', 'None'),
            'drugs': p.get('drugs'),
            'disease_code' : p['disease_code']
        }
        all_patient_annotations.append(parsed)

    keys = all_patient_annotations[0].keys()
    os.chdir('../../..')
    subpath = 'hail_data_tmp/TCGA'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
    else:
        os.makedirs(subpath)
    os.chdir(subpath)
    f = open("tcga_annotations.txt", "w")
    f.write('\t'.join(keys) + '\n')
    for item in all_patient_annotations:
        f.write('\t'.join([str(val) for val in item.values()]) + '\n')
    f.close()

def download():
    subpath = 'gene_data_tmp/TCGA'
    if os.path.isdir(subpath):
        print(f"Working under {subpath}")
    else:
        os.makedirs(subpath)
    os.chdir(subpath)
    filename = os.path.basename(TCGA_URL)
    if os.path.isfile(filename):
        print(f"File {filename} already exist, skipping downloading")
    else:
        print("Downloading data file")
        subprocess.check_call('wget '+TCGA_URL, shell=True)
        filename = os.path.basename(TCGA_URL)
        print(f"Decompressing {filename}")
        subprocess.check_call(f"tar zxf {filename} --skip-old-files", shell=True)

def main():
    download()
    write_hail_TCGA_annotation_files()

if __name__ == '__main__':
    main()
