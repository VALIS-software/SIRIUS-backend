import os
from sirius.parsers.Parser import Parser
from sirius.helpers.constants import CHROMO_IDXS, DATA_SOURCE_GWAS

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
        2. No GenomeNodes are generated here because the SNPs should be imported from dbSNP.
        3. Each study is parsed as one or more Edges, assuming the SNPs and traits are already in the database.
        4. The InfoNodes generated contain 1 dataSource and multiple traits. They should all have "_id" values start with "I", like "IGWAS", or "Itrait_we9w485.."
        The _id for the traits are computed as a hash of the trait name in lower case. Duplicated traits with the same _id are ignored.
        5. The Edges generated are connections between the SNPs and traits. Each edge should have an "_id" starts with "E", and "from_id" = the SNP's _id,
        and "to_id" = the trait's _id. The _id for the traits are computed as a hash of the trait description in lower case, to match the ones generated in OBOParser_EFO.
        6. All edges have the type "association:SNP:trait". Details of the GWAS studies are put in to the 'info' block of the edges.
        The 'info.p-value' is extracted from the "P-VALUE" column of the data table, and coverted to float points here.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = GWASParser('GWAS.tsv')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        No GenomeNodes are generated here because the SNPs should be imported from dbSNP:

        >>> print(genome_nodes)
        []

        The first InfoNode is the dataSource:

        >>> print(info_nodes[0])
        {
            "_id": "IGWAS",
            "type": "dataSource",
            "name": "GWAS",
            "source": "GWAS",
            "info": {
                "filename": "GWAS.tsv"
            }
        }

        The rest of InfoNodes are the traits:

        >>> print(info_nodes[1])
        {
            "_id": "Itrait5d79257733a02120e530fc22213ff748cfa18f30d524088652cf94375708b23f",
            "name": "atopic eczema",
            "source": "GWAS",
            "info": {
                "EFO": "0000274",
                "description": "Inflammatory skin disease"
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
        known_traits = set()
        for study_data in self.studies:
            study = study_data.copy()
            # there might be multiple snps related, therefore we split them
            snps = study.pop("SNPS").split(';')
            snp_ids = []
            for snp_id in snps:
                rs = snp_id.strip().lower()
                if rs[:2] != 'rs': continue # we skip some non standard IDs for now
                rs = rs[2:]
                rs_id = 'Gsnp_rs' + rs
                snp_ids.append(rs_id)
            # there might be multiple traits, compute the trait ids from there MAPPED_TRAIT
            mapped_trait = study.pop("MAPPED_TRAIT")
            # we skip the studies with no mapped traits
            if not mapped_trait: continue
            trait_names = mapped_trait.split(',')
            trait_ids = ['Itrait' + self.hash(t.strip().lower()) for t in trait_names]
            trait_URIs = study.pop("MAPPED_TRAIT_URI").split(',')
            for traitid, name, uri in zip(trait_ids, trait_names, trait_URIs):
                if traitid not in known_traits:
                    known_traits.add(traitid)
                    baseuri = os.path.basename(uri)
                    urikey, urivalue = baseuri.split('_')
                    info_node = {
                        '_id': traitid,
                        'name': name,
                        'source': DATA_SOURCE_GWAS,
                        'info': {
                            urikey: urivalue,
                            'description': study["DISEASE/TRAIT"]
                        }
                    }
                    info_nodes.append(info_node)
            # format edge
            edge = {
                'type': 'association:SNP:trait',
                'source': DATA_SOURCE_GWAS,
                'name': 'GWAS Study',
                'info': study.copy(),
            }
            edge['info']['p-value'] = float(edge['info'].pop('P-VALUE', 0))
            edge['info']['description'] = edge['info'].pop('STUDY')
            # create edges between each snp and each trait
            for snpid in snp_ids:
                for traitid in trait_ids:
                    this_edge = edge.copy()
                    this_edge.update({
                        'from_id': snpid,
                        'to_id': traitid,
                    })
                    this_edge['_id'] = 'E' + self.hash(str(this_edge))
                    edges.append(this_edge)
        return genome_nodes, info_nodes, edges
