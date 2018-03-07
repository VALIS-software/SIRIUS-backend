#!/bin/bash

echo "***** Genome Data Download Script *****"
echo
echo "Please make sure you're in a folder that has enough space"
echo "To save all the original and parsed data files, it's preferred to have > 10 GB free space"
echo
read -p "Press enter to continue"

# download GRCh38 data in GFF3 format
echo "Downloading GRCh38 annotation data in GRCh38_gff folder"
mkdir GRCh38_gff; cd GRCh38_gff
wget ftp://ftp.ncbi.nlm.nih.gov/refseq/H_sapiens/annotation/GRCh38_latest/refseq_identifiers/GRCh38_latest_genomic.gff.gz
# decompress
gzip -d GRCh38_latest_genomic.gff.gz
cd ..

# download GWAS data
echo "Downloading GWAS data in gwas folder"
mkdir gwas; cd gwas
curl -o gwas.tsv https://www.ebi.ac.uk/gwas/api/search/downloads/full
cd ..

# downlaod eQTL data
echo "Downloading eQTL data in eQTL folder"
mkdir eQTL; cd eQTL
wget http://www.exsnp.org/data/GSexSNP_allc_allp_ld8.txt
cd ..

echo "All downloads finished!"
