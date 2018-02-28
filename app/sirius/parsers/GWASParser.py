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
            labels = f.readline()[:-1].split('\t')
            print(labels)
            for line in f:
                ls = line[:-1].split('\t')
                self.studies.append(dict(zip(labels, ls)))
                if self.verbose and len(self.studies) % 100000 == 0:
                    print("%d data parsed" % len(self.studies), end='\r')
        self.data = {'metadata': self.metadata, 'studies': self.studies}

    def get_mongo_nodes(self):
        """ Parse study data into three types of nodes """
        # GenomeNode: Information about SNP, unique ID is defined based on rs number
        #    {
        #      "_id": "snp_rs4950928",
        #      "type": "SNP",
        #      "chromid": 1,
        #      "start": 203186754,
        #      "end": 203186754,
        #      "length": 1,
        #      "sourceurl": "gwas_catalog_v1.0.1-associations_e91_r2018-02-13.tsv",
        #      "assembly": "GRCh38",
        #      "info": {
        #        "ID": "rs4950928",
        #        "name": "rs4950928",
        #        "mapped_gene": "CHI3L1"
        #      }
        #    },
        #
        # InfoNode: Information about trait, unique ID is defined based on trait name
        #    {
        #      "_id": "trait_ykl-40 levels",
        #      "type": "trait",
        #      "name": "ykl-40 levels",
        #      "sourceurl": "gwas_catalog_v1.0.1-associations_e91_r2018-02-13.tsv"
        #    }
        #
        # EdgeNode: Study that connects SNP and trait
        # {
        #       "from_id": "snp_rs4950928",
        #       "to_id": "trait_ykl-40 levels",
        #       "from_type": "SNP",
        #       "to_type": "trait",
        #       "type": "association",
        #       "sourceurl": "gwas_catalog_v1.0.1-associations_e91_r2018-02-13.tsv",
        #       "info": {
        #         "DATE ADDED TO CATALOG": "2009-09-28",
        #         "PUBMEDID": "18403759",
        #         "FIRST AUTHOR": "Ober C",
        #         "DATE": "2008-04-09",
        #         "JOURNAL": "N Engl J Med",
        #         "LINK": "www.ncbi.nlm.nih.gov/pubmed/18403759",
        #         "STUDY": "Effect of variation in CHI3L1 on serum YKL-40 level, risk of asthma, and lung function.",
        #         "DISEASE/TRAIT": "YKL-40 levels",
        #         "INITIAL SAMPLE SIZE": "632 Hutterite individuals",
        #         "REPLICATION SAMPLE SIZE": "443 European ancestry cases, 491 European ancestry controls, 206 European ancestry individuals",
        #         "REGION": "1q32.1",
        #         "REPORTED GENE(S)": "CHI3L1",
        #         "UPSTREAM_GENE_ID": "",
        #         "DOWNSTREAM_GENE_ID": "",
        #         "SNP_GENE_IDS": "1116",
        #         "UPSTREAM_GENE_DISTANCE": "",
        #         "DOWNSTREAM_GENE_DISTANCE": "",
        #         "STRONGEST SNP-RISK ALLELE": "rs4950928-G",
        #         "MERGED": "0",
        #         "SNP_ID_CURRENT": "4950928",
        #         "CONTEXT": "upstream_gene_variant",
        #         "INTERGENIC": "0",
        #         "RISK ALLELE FREQUENCY": "0.29",
        #         "PVALUE_MLOG": "13.0",
        #         "P-VALUE (TEXT)": "",
        #         "OR or BETA": "0.3",
        #         "95% CI (TEXT)": "[NR] ng/ml decrease",
        #         "PLATFORM [SNPS PASSING QC]": "Affymetrix [290325]",
        #         "CNV": "N",
        #         "MAPPED_TRAIT": "YKL40 measurement",
        #         "MAPPED_TRAIT_URI": "http://www.ebi.ac.uk/efo/EFO_0004869",
        #         "STUDY ACCESSION": "GCST000177",
        #         "p-value": 1e-13
        #       }
        # }
        if hasattr(self, 'mongonodes'): return self.mongonodes
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
                infonode = { '_id': trait_id,
                             'type': 'trait',
                             'name': trait,
                             'sourceurl': sourceurl
                           }
                info_nodes.append(infonode)
                known_traits.add(trait_id)
            # there might be multiple snps related, therefore we split them
            snps = study.pop("SNPS").split(';')
            CHR_IDs = study.pop("CHR_ID").split(';')
            CHR_POSs = study.pop("CHR_POS").split(';')
            if not len(snps) == len(CHR_IDs) == len(CHR_POSs):
                print('Skipped because snp record length not matching \n %s' % str(study))
                continue
            MAPPED_GENEs = study.pop("MAPPED_GENE").split(';')
            this_snp_ids = []
            for i, snp_id in enumerate(snps):
                rs = snp_id.strip().lower()
                if rs[:2] != 'rs': continue # we skip some non standard IDs for now
                if CHR_IDs[i] not in chromo_idxs:
                    print("Skipped because CHR_ID %s not known" % CHR_IDs[i])
                    continue
                else:
                    chromid = chromo_idxs[CHR_IDs[i]]
                rs_id = 'snp_' + rs
                this_snp_ids.append(rs_id)
                # add SNP to genome_nodes
                if rs_id not in known_rs:
                    known_rs.add(rs_id)
                    pos = int(CHR_POSs[i])
                    mapped_gene = MAPPED_GENEs[i]
                    gnode = { '_id': rs_id,
                              'type': 'SNP',
                              'chromid': chromid,
                              'start': pos,
                              'end': pos,
                              'length': 1,
                              'sourceurl': sourceurl,
                              'assembly': "GRCh38",
                              'info': {'ID': rs, 'name': rs}
                            }
                    try:
                        gnode['info']['mapped_gene'] = MAPPED_GENEs[i]
                    except:
                        pass
                    genome_nodes.append(gnode)

            # add edge node for each SNP
            for rs_id in this_snp_ids:
                # parse pvalue
                try:
                    study['p-value'] = float(study.pop('P-VALUE'))
                except:
                    pass
                # add study to edge_nodes
                edgenode = {'from_id': rs_id , 'to_id': trait_id,
                            'from_type': 'SNP', 'to_type': 'trait',
                            'type': 'association',
                            'sourceurl': sourceurl,
                            'info': study
                           }
                edge_nodes.append(edgenode)
        self.mongonodes = genome_nodes, info_nodes, edge_nodes
        return self.mongonodes
