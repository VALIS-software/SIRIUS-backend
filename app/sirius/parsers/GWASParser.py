#!/usr/bin/env python

import os, sys
import json
from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, chromo_names

class GWASParser(Parser):

    def parse(self):
        """ Parse the GWAS tsv data format. Ref: https://www.ebi.ac.uk/gwas/docs/fileheaders#_file_headers_for_catalog_version_1_0 """
        # Example:
        # self.studies = {
        #     "DATE ADDED TO CATALOG": "2009-09-28",
        #     "PUBMEDID": "18403759",
        #     "FIRST AUTHOR": "Ober C",
        #     "DATE": "2008-04-09",
        #     "JOURNAL": "N Engl J Med",
        #     "LINK": "www.ncbi.nlm.nih.gov/pubmed/18403759",
        #     "STUDY": "Effect of variation in CHI3L1 on serum YKL-40 level, risk of asthma, and lung function.",
        #     "DISEASE/TRAIT": "YKL-40 levels",
        #     "INITIAL SAMPLE SIZE": "632 Hutterite individuals",
        #     "REPLICATION SAMPLE SIZE": "443 European ancestry cases, 491 European ancestry controls, 206 European ancestry individuals",
        #     "REGION": "1q32.1",
        #     "CHR_ID": "1",
        #     "CHR_POS": "203186754",
        #     "REPORTED GENE(S)": "CHI3L1",
        #     "MAPPED_GENE": "CHI3L1",
        #     "UPSTREAM_GENE_ID": "",
        #     "DOWNSTREAM_GENE_ID": "",
        #     "SNP_GENE_IDS": "1116",
        #     "UPSTREAM_GENE_DISTANCE": "",
        #     "DOWNSTREAM_GENE_DISTANCE": "",
        #     "STRONGEST SNP-RISK ALLELE": "rs4950928-G",
        #     "SNPS": "rs4950928",
        #     "MERGED": "0",
        #     "SNP_ID_CURRENT": "4950928",
        #     "CONTEXT": "upstream_gene_variant",
        #     "INTERGENIC": "0",
        #     "RISK ALLELE FREQUENCY": "0.29",
        #     "P-VALUE": "1E-13",
        #     "PVALUE_MLOG": "13",
        #     "P-VALUE (TEXT)": "",
        #     "OR or BETA": "0.3",
        #     "95% CI (TEXT)": "[NR] ng/ml decrease",
        #     "PLATFORM [SNPS PASSING QC]": "Affymetrix [290325]",
        #     "CNV\n": "N\n"
        # }


        self.metadata = {'filename': self.filename}
        self.studies = []
        with open(self.filename) as f:
            labels = f.readline().split('\t')
            print(labels)
            for line in f:
                ls = line.split('\t')
                self.studies.append(dict(zip(labels, ls)))
                if self.verbose and len(self.studies) % 100000 == 0:
                    print("%d data parsed" % len(self.studies), end='\r')
        self.data = {'metadata': self.metadata, 'studies': self.studies}

    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes:
        GenomeNode: Information about SNP, unique ID is defined based on rs number
            Example: { '_id': 'snp_rs4950928',
                       'type': 'SNP',
                       'location': 'Chr1',
                       'start': 203186754,
                       'end': 203186754,
                       'length': 1,
                       'sourceurl': None,
                       'assembly': None,
                       'info': {'ID': 'rs4950928',
                                'Name': 'rs4950928',
                                'mapped_gene': 'CHI3L1',
                               }
                     }
        InfoNode: Information about trait, unique ID is defined based on trait name
            Example: { '_id': 'trait_psoriasis', 'type': 'trait', info: {'name': 'psoriasis'} }
        EdgeNode: Study that connects SNP and trait
            Example: { 'from_id': 'snp_rs4950928', to_id: 'trait_psoriasis',
                       'from_type': 'SNP', 'to_type': 'trait',
                       'type': 'association',
                       'sourceurl': 'https://www.ebi.ac.uk/gwas/docs/file-downloads',
                       'info': { "DATE ADDED TO CATALOG": "2009-09-28"
                                 "PUBMEDID": "18403759",
                                 "FIRST AUTHOR": "Ober C",
                                 "DATE": "2008-04-09",
                                 "JOURNAL": "N Engl J Med",
                                 "STUDY": "Effect of variation in CHI3L1 on serum YKL-40 level, risk of asthma, and lung function.",
                                 "DISEASE/TRAIT": "YKL-40 levels",
                                 "INITIAL SAMPLE SIZE": "632 Hutterite individuals",
                                 "REPLICATION SAMPLE SIZE": "443 European ancestry cases, 491 European ancestry controls, 206 European ancestry individuals",
                                 "REGION": "1q32.1",
                                 "CHR_ID": "1",
                                 "CHR_POS": "203186754",
                                 "REPORTED GENE(S)": "CHI3L1",
                                 "MAPPED_GENE": "CHI3L1",
                                 "UPSTREAM_GENE_ID": "",
                                 "DOWNSTREAM_GENE_ID": "",
                                 "SNP_GENE_IDS": "1116",
                                 "UPSTREAM_GENE_DISTANCE": "",
                                 "DOWNSTREAM_GENE_DISTANCE": "",
                                 "STRONGEST SNP-RISK ALLELE": "rs4950928-G",
                                 "SNPS": "rs4950928",
                                 "MERGED": "0",
                                 "SNP_ID_CURRENT": "4950928",
                                 "CONTEXT": "upstream_gene_variant",
                                 "INTERGENIC": "0",
                                 "RISK ALLELE FREQUENCY": "0.29",
                                 "P-VALUE": "1E-13",
                                 "PVALUE_MLOG": "13",
                                 "P-VALUE (TEXT)": "",
                                 "OR or BETA": "0.3",
                                 "95% CI (TEXT)": "[NR] ng/ml decrease",
                                 "PLATFORM [SNPS PASSING QC]": "Affymetrix [290325]",
                                 "CNV\n": "N\n"
                       }
                     }
        """
        genome_nodes, info_nodes, edge_nodes = [], [], []
        known_rs, known_traits = set(), set()
        if 'sourceurl' in self.metadata:
            sourceurl = self.metadata['sourceurl']
        else:
            sourceurl = self.filename
        for study in self.studies:
            trait = study["DISEASE/TRAIT"].lower()
            trait_id = 'trait_'+trait
            if trait_id not in known_traits:
                infonode = { '_id': trait_id, 'type': 'trait', 'name': trait }
                info_nodes.append(infonode)
                known_traits.add(trait_id)
            # there might be multiple snps related, therefore we split them
            snps = study["SNPS"].split(';')
            CHR_IDs = study["CHR_ID"].split(';')
            CHR_POSs = study["CHR_POS"].split(';')
            if not len(snps) == len(CHR_IDs) == len(CHR_POSs):
                print('Skipped because snp record length not matching \n %s' % str(study))
                continue
            # there might be error in the MAPPED_GENE key, ingore it if error
            MAPPED_GENEs = study["MAPPED_GENE"].split(';')
            if len(MAPPED_GENEs) != len(snps):
                MAPPED_GENEs = [None] * len(snps)
            for i, snp_id in enumerate(snps):
                rs = snp_id.strip().lower()
                if rs[:2] == 'rs':
                    rs_id = 'snp_' + rs
                    # add SNP to genome_nodes
                    if rs_id not in known_rs:
                        known_rs.add(rs_id)
                        location = 'Chr' + CHR_IDs[i].strip()
                        if location in chromo_idxs.keys():
                            pos = int(CHR_POSs[i])
                            mapped_gene = MAPPED_GENEs[i]
                            gnode = { '_id': rs_id,
                                       'type': 'SNP',
                                       'location': location,
                                       'start': pos,
                                       'end': pos,
                                       'length': 1,
                                       'sourceurl': sourceurl,
                                       'assembly': "GRCh38",
                                       'info': {'ID': rs,
                                                'Name': rs,
                                                'mapped_gene': mapped_gene
                                               }
                                     }
                            genome_nodes.append(gnode)
                    # parse pvalue
                    try:
                        study['pvalue'] = float(study['P-VALUE'])
                    except:
                        study['pvalue'] = None
                    # add study to edge_nodes
                    edgenode = {'from_id': rs_id , 'to_id': trait_id,
                                'from_type': 'SNP', 'to_type': 'trait',
                                'type': 'association',
                                'sourceurl': sourceurl,
                                'info': study
                               }
                    edge_nodes.append(edgenode)
        return genome_nodes, info_nodes, edge_nodes

    def save_mongo_nodes(self, filename=None):
        if filename == None: filename = self.filename + '.mongonodes'
        genome_nodes, info_nodes, edge_nodes = self.get_mongo_nodes()
        d = {'genome_nodes': genome_nodes, 'info_nodes': info_nodes, 'edge_nodes': edge_nodes}
        json.dump(d, open(filename, 'w'), indent=2)
