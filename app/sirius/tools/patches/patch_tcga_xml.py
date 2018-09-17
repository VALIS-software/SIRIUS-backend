#!/usr/bin/env python

import os
from sirius.mongo import InfoNodes
from sirius.parsers import TCGA_XMLParser
from sirius.mongo.upload import update_insert_many

def locate_folder():
    if 'gene_data_tmp' in os.listdir('.'):
        os.chdir('gene_data_tmp')
    if 'TCGA' in os.listdir('.'):
        os.chdir('TCGA')
    assert 'BCRXML' in os.listdir('.'), 'Can not find BCRXML/ folder'

def remove():
    ret = InfoNodes.delete_many({'type': 'patient', 'source': 'TCGA'})
    print(f"deleted {ret.deleted_count} TCGA patient infonodes")

def upload():
    # XML for patient info
    os.chdir('BCRXML')
    xml_files = []
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.xml'):
                xml_files.append(os.path.join(root, f))
    xml_files.sort()
    all_patient_infonodes = []
    # this is used in MAF parser
    patient_barcode_tumor_site = dict()
    # these are used in CNV parser
    patient_uuid_tumor_site = dict()
    patient_uuid_barcode = dict()
    print(f"Parsing {len(xml_files)} patient xml files")
    for f in xml_files:
        parser = TCGA_XMLParser(f, verbose=True)
        parser.parse()
        genome_nodes, info_nodes, edges = parser.get_mongo_nodes()
        # record the tumor site for each patient barcode
        info = info_nodes[0]['info']
        patient_barcode = info['patient_barcode']
        patient_barcode_tumor_site[patient_barcode] = info['biosample']
        patient_uuid = info['patient_uuid']
        patient_uuid_tumor_site[patient_uuid] = info['biosample']
        patient_uuid_barcode[patient_uuid] = patient_barcode
        # collection individual info_nodes for each patient
        all_patient_infonodes += info_nodes
    # upload all patient info_nodes at once
    update_insert_many(InfoNodes, all_patient_infonodes)

def main():
    locate_folder()
    remove()
    upload()

if __name__ == '__main__':
    main()