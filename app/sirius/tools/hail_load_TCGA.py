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
    n_sample = len(maf_files)
    vcf_row_headers = []
    vcf_data_mat = []
    vcf_id_index = dict()
    sample_ids = []
    for i_sample, f in enumerate(maf_files[:10]):
        parser = TCGA_MAFParser(f)
        parser.parse()
        sample_ids.append(parser.mutations[0]['Tumor_Sample_Barcode'])
        n_new, n_exist = 0, 0
        for vcf_str in parser.vcf_generator():
            (chrom, pos, gid, ref, alt, qual, filt, info, fmt, sample) = vcf_str.split('\t')
            if gid not in vcf_id_index:
                # insert a new row
                row_header = [chrom, pos, gid, ref, alt, qual, filt, info, fmt]
                vcf_row_headers.append('\t'.join(row_header))
                row_data = np.full(n_sample, '.', dtype='|U6') # string limited to length 6
                row_data[i_sample] = sample
                vcf_data_mat.append(row_data)
                vcf_id_index[gid] = len(vcf_data_mat)
                n_new += 1
            else:
                # update the data
                vcf_data_mat[vcf_id_index[gid]][i_sample] = sample
                n_exist += 1
        print(f"{i_sample:3d}| new {n_new:10d} | exist {n_exist:10d} | accu {len(vcf_data_mat)}")
    # make the header
    header_lines = parser.vcf_header().split('\n')
    label_line = header_lines[-1]
    # remove the last sample id, add all ids
    labels = label_line.split('\t')[:-1] + sample_ids
    header_lines = header_lines[:-1] + ['\t'.join(labels)]
    header = '\n'.join(header_lines) + '\n'
    # write all variants data into file
    os.chdir(current_dir)
    filename = "tcga_variants.vcf"
    fullpath = os.path.join(hail_folder, filename)
    with open(fullpath, "w") as afile:
        afile.write(header)
        for rowh, data in zip(vcf_row_headers, vcf_data_mat):
            afile.write(rowh + '\t' + '\t'.join(data) + '\n')
    print(f"Writing {fullpath} finished.")


def main():
    hail_folder = create_hail_tmp_folder()
    download_folder = find_tcga_folder()
    check_download(download_folder)
    #write_hail_TCGA_patient_annotation_file(hail_folder, download_folder)
    write_hail_TCGA_VCF(hail_folder, download_folder)

if __name__ == '__main__':
    main()
