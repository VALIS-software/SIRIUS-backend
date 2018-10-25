#!/usr/bin/env hail

import os
import shutil
import hail as hl


hl.init(default_reference='GRCh38')

ds = hl.import_vcf('/cache/hail_data_tmp/tcga_variants.vcf')

new = ds.rename({'s': 'patient_barcode'})

table = (hl.import_table('/cache/hail_data_tmp/patient_annotations.txt', impute=True)).key_by('patient_barcode')

test = new.annotate_cols(**table[new.patient_barcode])

mt_folder = '/cache/hail_data_tmp/tcga.mt'
if os.path.exists(mt_folder):
    shutil.rmtree(mt_folder)
test.write(mt_folder)
