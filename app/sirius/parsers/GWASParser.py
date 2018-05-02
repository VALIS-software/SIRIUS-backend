from sirius.parsers.Parser import Parser
from sirius.realdata.constants import CHROMO_IDXS, DATA_SOURCE_GWAS

class GWASParser(Parser):
    """
    Parser for the GWAS .tsv file format

    Parameters
    ----------
    filename: string
        The name of the file to be parsed.
    verbose: boolean, optional
        The flag that enables printing verbose information during parsing.
        Default is False.

    Attributes
    ----------
    filename: string
        The filename which `Parser` was initialized.
    ext: string
        The extension of the file the `Parser` was initialized.
    data: dictionary
        The internal object hold the parsed data.
    metadata: dictionary
        Points to self.data['metadata'], initilized as metadata = {'filename': filename}
    filehandle: _io.TextIOWrapper
        The filehanlde openned for self.filename.
    verbose: boolean
        The flag that enables printing verbose information during parsing.

    Methods
    -------
    parse
    get_mongo_nodes
    * inherited from parent class *
    jsondata
    save_json
    load_json
    save_mongo_nodes
    hash

    Notes
    -----
    1. The .tsv contain tab-separated values in lines.
    2. We assume the first line of the file contains the labels, and the number of labels should match the number of values in each line.
    3. No comment lines are expected.

    References
    ----------
    https://www.ebi.ac.uk/gwas/docs/fileheaders#_file_headers_for_catalog_version_1_0

    Examples
    --------
    Initiate a GWASParser:

    >>> parser = GWASParser("gwas.tsv")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    Get the Mongo nodes

    >>> mongo_nodes = parser.get_mongo_nodes()

    Save the Mongo nodes to a file

    >>> parser.save_mongo_nodes('output.mongonodes')

    """

    @property
    def studies(self):
        return self.data['studies']

    @studies.setter
    def studies(self, value):
        self.data['studies'] = value

    def parse(self):
        """
        Parse the GWAS tsv data format.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The .tsv contain tab-separated values in lines.
        3. We assume the first line of the file contains the labels, and the number of labels should match the number of values in each line.
        4. No comment lines are expected.

        References
        ----------
        https://www.ebi.ac.uk/gwas/docs/fileheaders#_file_headers_for_catalog_version_1_0

        Examples
        --------
        Initialize and parse the file:

        >>> parser = GWASParser('GWAS.tsv')
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.studies:

        >>> parser.studies[0]
        {
            "DATE ADDED TO CATALOG": "2009-09-28",
            "PUBMEDID": "18403759",
            "FIRST AUTHOR": "Ober C",
            "DATE": "2008-04-09",
            "JOURNAL": "N Engl J Med",
            "LINK": "www.ncbi.nlm.nih.gov/pubmed/18403759",
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
            "CNV": "N"
        }

        """
        # start from the beginning for reading
        self.filehandle.seek(0)
        self.studies = []
        # read the first line as labels
        labels = self.filehandle.readline()[:-1].split('\t')
        # read the rest of the lines as data
        for line in self.filehandle:
            ls = line[:-1].split('\t')
            self.studies.append(dict(zip(labels, ls)))
            if self.verbose and len(self.studies) % 100000 == 0:
                print("%d data parsed" % len(self.studies), end='\r')

    def get_mongo_nodes(self):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.

        Returns
        -------
        mongonodes: tuple
            The return tuple is (genome_nodes, info_nodes, edges)
            Each of the three is a list of multiple dictionaries, which contains the parsed data.

        Notes
        -----
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.studies,
        which are contents of self.data
        2. The GenomeNodes generated from this parsing should be SNPs. They should all have "_id" started with "G", like "Gsnp_rs4950928".
        Duplicated SNPs with the same _id are ignored.
        3. Each study is parsed as one trait, that might be associated with multiple SNPs.
        4. The InfoNodes generated contain 1 dataSource and multiple traits. They should all have "_id" values start with "I", like "IGWAS", or "Itrait_79485.."
        The _id for the traits are computed as a hash of the trait description in lower case. Duplicated traits with the same _id are ignored.
        5. The Edges generated are connections between the SNPs and traits. Each edge should have an "_id" starts with "E", and "from_id" = the SNP's _id,
        and "to_id" = the trait's _id. All edges have the type "association" for now. Details of the GWAS studies are put in to the 'info' block of the edges.
        The 'info.p-value' is extracted from the "P-VALUE" column of the data table, and coverted to float points here.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = GWASParser('GWAS.tsv')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        The GenomeNodes contain the information for the SNPs:

        >>> print(genome_nodes[0])
        {
            "_id": "Gsnp_rs4950928",
            "assembly": "GRCh38",
            "type": "SNP",
            "chromid": 1,
            "start": 203186754,
            "end": 203186754,
            "length": 1,
            "source": "GWAS",
            "name": "RS4950928",
            "info": {
                "ID": "4950928",
                "description": "SNP 4950928",
                "mapped_gene": "CHI3L1"
            }
        }

        The first InfoNode is the dataSource:

        >>> print(info_nodes[0])
        {
            "_id": "IGWAS",
            "type": "dataSource",
            "name": "GWAS",
            "source": "GWAS",
            "info": {
                "filename": "test_GWAS.tsv"
            }
        }

        The rest of the InfoNodes are traits:

        >>> print(info_nodes[1])
        {
            "_id": "Itrait_7948525417e53a1f7da272315d6fd30ae1214e6264d1e70b415e1846db81495d",
            "type": "trait",
            "name": "YL",
            "source": "GWAS",
            "info": {
                "description": "YKL-40 levels"
            }
        }

        The Edges contain details of the GWAS studies:

        >>> print(edges[0])
        {
            "from_id": "Gsnp_rs4950928",
            "to_id": "Itrait_7948525417e53a1f7da272315d6fd30ae1214e6264d1e70b415e1846db81495d",
            "type": "association",
            "source": "GWAS",
            "name": "GWAS",
            "info": {
                "DATE ADDED TO CATALOG": "2009-09-28",
                "PUBMEDID": "18403759",
                "FIRST AUTHOR": "Ober C",
                "DATE": "2008-04-09",
                "JOURNAL": "N Engl J Med",
                "LINK": "www.ncbi.nlm.nih.gov/pubmed/18403759",
                "DISEASE/TRAIT": "YKL-40 levels",
                "INITIAL SAMPLE SIZE": "632 Hutterite individuals",
                "REPLICATION SAMPLE SIZE": "443 European ancestry cases, 491 European ancestry controls, 206 European ancestry individuals",
                "REGION": "1q32.1",
                "REPORTED GENE(S)": "CHI3L1",
                "UPSTREAM_GENE_ID": "",
                "DOWNSTREAM_GENE_ID": "",
                "SNP_GENE_IDS": "1116",
                "UPSTREAM_GENE_DISTANCE": "",
                "DOWNSTREAM_GENE_DISTANCE": "",
                "STRONGEST SNP-RISK ALLELE": "rs4950928-G",
                "MERGED": "0",
                "SNP_ID_CURRENT": "4950928",
                "CONTEXT": "upstream_gene_variant",
                "INTERGENIC": "0",
                "RISK ALLELE FREQUENCY": "0.29",
                "PVALUE_MLOG": "13",
                "P-VALUE (TEXT)": "",
                "OR or BETA": "0.3",
                "95% CI (TEXT)": "[NR] ng/ml decrease",
                "PLATFORM [SNPS PASSING QC]": "Affymetrix [290325]",
                "CNV": "N",
                "p-value": 1e-13,
                "description": "Effect of variation in CHI3L1 on serum YKL-40 level, risk of asthma, and lung function."
            },
            "_id": "Ed99ad04f5210c7899abb1d7bf13981233f89330c14133b3cb3156da993130349"
        }

        """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node = {"_id": 'I'+DATA_SOURCE_GWAS, "type": "dataSource", "name": DATA_SOURCE_GWAS, "source": DATA_SOURCE_GWAS}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        known_rs, known_traits, known_edge_ids = set(), set(), set()
        for study_data in self.studies:
            study = study_data.copy()
            trait = study["DISEASE/TRAIT"]
            trait_id = 'Itrait_'+self.hash(trait.lower())
            short_name = (''.join(s[0] for s in trait.split())).upper()
            if trait_id not in known_traits:
                infonode = { '_id': trait_id,
                             'type': 'trait',
                             'name': short_name,
                             'source': DATA_SOURCE_GWAS,
                             'info': {
                                 'description': trait
                             }
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
                rs = rs[2:]
                if CHR_IDs[i] not in CHROMO_IDXS:
                    print("Skipped because CHR_ID %s not known" % CHR_IDs[i])
                    continue
                else:
                    chromid = CHROMO_IDXS[CHR_IDs[i]]
                rs_id = 'Gsnp_rs' + rs
                this_snp_ids.append(rs_id)
                name = 'RS' + rs
                # add SNP to genome_nodes
                if rs_id not in known_rs:
                    known_rs.add(rs_id)
                    pos = int(CHR_POSs[i])
                    mapped_gene = MAPPED_GENEs[i]
                    gnode = {
                        '_id': rs_id,
                        'assembly': "GRCh38",
                        'type': 'SNP',
                        'chromid': chromid,
                        'start': pos,
                        'end': pos,
                        'length': 1,
                        'source': DATA_SOURCE_GWAS,
                        'name': name,
                        'info': {
                            'ID': rs,
                            'description': 'SNP ' + rs
                        }
                    }
                    try:
                        gnode['info']['mapped_gene'] = MAPPED_GENEs[i]
                    except:
                        pass
                    genome_nodes.append(gnode)
            # add edge node for each SNP
            for rs_id in this_snp_ids:
                # add study to edges
                edge = {'from_id': rs_id , 'to_id': trait_id,
                        'type': 'association',
                        'source': DATA_SOURCE_GWAS,
                        'name': 'GWAS',
                        'info': study.copy(),
                       }
                # parse pvalue
                try:
                    edge['info']['p-value'] = float(edge['info'].pop('P-VALUE'))
                except:
                    pass
                edge['info']['description'] = edge['info'].pop('STUDY')
                edge['_id'] = 'E' + self.hash(str(edge))
                if edge['_id'] not in known_edge_ids:
                    known_edge_ids.add(edge['_id'])
                    edges.append(edge)
        return genome_nodes, info_nodes, edges
