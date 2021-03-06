#!/usr/bin/env python

import os
import subprocess
import numpy as np
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
        'age': (lambda p: -float(p['days_to_birth'])/365.0 if p.get('days_to_birth', None) != None else -1.0),
        'gender': (lambda p: p['gender']),
        'biosample': (lambda p: p.get('tumor_tissue_site', 'None')),
        'vital_status': (lambda p: p['vital_status']),
        'days_to_death': (lambda p: int(p['days_to_death']) if p.get('days_to_death', None) != None else -1),
        'days_to_last_followup': (lambda p: int(p['days_to_last_followup']) if p.get('days_to_last_followup', None) != None else -1),
        'histological_type': (lambda p: p.get('histological_type', 'None')),
        'drugs': (lambda p: str(p['drugs']).strip()),
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
    if os.path.isdir('BCRXML') and os.path.isdir('MAF'):
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

def write_hail_TCGA_VCF(hail_folder, download_folder):
    current_dir = os.getcwd()
    # MAF for mutations in tumors
    os.chdir(os.path.join(download_folder, 'MAF'))
    maf_files = []
    for root, d, files in os.walk('.'):
        for f in files:
            if f.endswith('.maf.gz'):
                maf_files.append(os.path.join(root, f))
    maf_files.sort()
    print(f"Parsing {len(maf_files)} maf files")
    # merge all maf files into a big VCF file
    vcf_row_headers = []
    vcf_data_mat = []
    vcf_id_index = dict()
    patient_bc_idx = dict()
    for i_file, f in enumerate(maf_files):
        parser = TCGA_MAFParser(f)
        parser.parse()
        n_new, n_exist = 0, 0
        for vcf_str in parser.vcf_generator():
            (chrom, pos, gid, ref, alt, qual, filt, info) = vcf_str.split('\t')
            # extract the last PATIENT_BARCODE from INFO field
            info, pb_field = info.rsplit(';', 1)
            # pb_field should be PB=TCGA-XX-XXXX
            patient_barcode = pb_field[3:]
            # if this patient_barcode is new, store the index of this patient (as the column index)
            patient_bc_idx.setdefault(patient_barcode, len(patient_bc_idx))
            patient_idx = patient_bc_idx[patient_barcode]
            if gid not in vcf_id_index:
                vcf_id_index[gid] = len(vcf_data_mat)
                # insert a new row
                row_header = [chrom, pos, gid, ref, alt, qual, filt, info]
                vcf_row_headers.append('\t'.join(row_header))
                row_data = [patient_idx]
                vcf_data_mat.append(row_data)
                n_new += 1
            else:
                # update the data of an existing row
                vcf_data_mat[vcf_id_index[gid]].append(patient_idx)
                n_exist += 1
        print(f"{i_file:3d}| new {n_new:10d} | exist {n_exist:10d} | total {n_new+n_exist:10d}")
    n_patients = len(patient_bc_idx)
    print(f'parsing finished, total {len(vcf_data_mat)} variants and {n_patients} patients')
    # make the header for the big VCF file
    header_lines = parser.vcf_header().split('\n')
    label_line = header_lines[-1]
    header_lines = header_lines[:-1]
    # add a "FORMAT" key as header line
    header_lines.append('##FORMAT=<ID=EXIST,Number=1,Type=Integer,Description="Indicate if this variant exist in this sample, 1 for True, 0 for False"\n')
    # add all patient_ids as samples into the label_line
    labels = label_line.split('\t') + ['FORMAT'] + list(patient_bc_idx.keys())
    label_line = '\t'.join(labels)
    header = '\n'.join(header_lines) + label_line + '\n'
    # write all variants data into file
    os.chdir(current_dir)
    filename = "tcga_variants.vcf"
    fullpath = os.path.join(hail_folder, filename)
    with open(fullpath, "w") as afile:
        afile.write(header)
        for rowh, row_data in zip(vcf_row_headers, vcf_data_mat):
            patient_data_list = ['.'] * n_patients
            for patient_idx in row_data:
                patient_data_list[patient_idx] = '1'
            afile.write(rowh + '\tEXIST\t' + '\t'.join(patient_data_list) + '\n')
    print(f"Writing {fullpath} finished.")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip_xml", action='store_true', help='skip the xml parsing')
    parser.add_argument('--skip_maf', action='store_true', help='skip the maf parsing')
    args = parser.parse_args()

    hail_folder = create_hail_tmp_folder()
    download_folder = find_tcga_folder()
    check_download(download_folder)
    if not args.skip_xml:
        write_hail_TCGA_patient_annotation_file(hail_folder, download_folder)
    if not args.skip_maf:
        write_hail_TCGA_VCF(hail_folder, download_folder)

if __name__ == '__main__':
    main()
