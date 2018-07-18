from sirius.parsers.Parser import Parser
from sirius.helpers.constants import DATA_SOURCE_GTEX, DATA_SOURCE_EXSNP

class EQTLParser(Parser):
    """
    Parser for the eQTL file format

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
    1. The eQTL .txt files are tab-separated, with data listed as lines.
    2. The first line of the file contains the labels of the columns.
    3. After parsing, self.eqtls will be a list of eqtl entries, each parsed from one line of data.
    4. The eQTL data file does not contain any metadata.
    5. The exSNP and exGENEID columns are required.

    Examples
    --------
    Initiate a EQTLParser:

    >>> parser = EQTLParser("gwas.tsv")

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
    def eqtls(self):
        return self.data['eqtls']

    @eqtls.setter
    def eqtls(self, value):
        self.data['eqtls'] = value

    def parse(self):
        """
        Parse the eQTL txt data format.
        This dataset contains associations between SNPs and Genes.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The eQTL .txt files are tab-separated, with data listed as lines.
        3. The first line of the file contains the labels of the columns.
        4. After parsing, self.eqtls will be a list of eqtl entries, each parsed from one line of data.
        5. The eQTL data file does not contain any metadata.
        6. The exSNP and exGENEID columns are required for later parsing into Mongo nodes.

        References
        ----------
        http://www.exsnp.org/eQTL

        Examples
        --------
        Initialize and parse the file:

        >>> parser = EQTLParser('Prostate.v7.egenes.txt.gz')
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.eqtls:

        >>> parser.eqtls[0]
        {
            "gene_id": "ENSG00000227232.4",
            "gene_name": "WASH7P",
            "gene_chr": "1",
            "gene_start": "14413",
            "gene_end": "29553",
            "strand": "-",
            "num_var": "1493",
            "beta_shape1": "1.09799",
            "beta_shape2": "210.597",
            "true_df": "95.584",
            "pval_true_df": "0.00617042",
            "variant_id": "1_1026071_A_C_b37",
            "tss_distance": "996518",
            "chr": "1",
            "pos": "1026071",
            "ref": "A",
            "alt": "C",
            "num_alt_per_site": "1",
            "rs_id_dbSNP147_GRCh37p13": "rs112305311",
            "minor_allele_samples": "4",
            "minor_allele_count": "5",
            "maf": "0.0189394",
            "ref_factor": "1",
            "pval_nominal": "0.00329429",
            "slope": "0.859833",
            "slope_se": "0.286178",
            "pval_perm": "0.710433",
            "pval_beta": "0.69258",
            "qval": "0.377421",
            "pval_nominal_threshold": "6.8408e-05",
            "log2_aFC": "1.053842",
            "log2_aFC_lower": "0.880039",
            "log2_aFC_upper": "1.257119"
        },

        """
        # start from the beginning for reading
        self.filehandle.seek(0)
        self.eqtls = []
        title = self.filehandle.readline().strip()
        labels = title.split('\t')
        for line in self.filehandle:
            line = line.strip() # remove '\n'
            if line:
                d = dict(zip(labels, line.split('\t')))
                self.eqtls.append(d)
                if self.verbose and len(self.eqtls) % 100000 == 0:
                    print("%d data parsed" % len(self.eqtls), end='\r')

class EQTLParser_GTEx(EQTLParser):
    def get_mongo_nodes(self, extra_info=None):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.

        Returns
        -------
        mongonodes: tuple
            The return tuple is (genome_nodes, info_nodes, edges)
            Each of the three is a list of multiple dictionaries, which contains the parsed data.

        References
        ----------
        https://www.gtexportal.org/home/documentationPage

        Notes
        -----
        1. The GTex data set contains relations between SNPs and genes.
        2. The InfoNodes generated here have only one entry, with the type dataSource. The _id field should have a string value starts with "I", like "IGTEx".
        3. All Edges here should have _id starts with "E". The "from_id" and "to_id" field are derived directly from the "gene_id" and
        "rs_id_dbSNP147_GRCh37p13" keys of the dataset.
        4. The first word in filename will be parsed as the info.biosample value

        Examples
        --------
        Initialize and parse the file:

        >>> parser = EQTLParser_GTex('Prostate.v7.egenes.txt.gz')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        No GenomeNodes are generated:

        >>> print(genome_nodes)
        []

        One InfoNode with type dataSource is generated:

        >>> print(info_nodes[0])
        {
            "_id": "IGTEx",
            "type": "dataSource",
            "name": "GTEx",
            "source": "GTEx",
            "info": {
                "filename": "Prostate.v7.egenes.txt.gz"
            }
        }

        Example of an Edge:

        >>> print(edges[0])
        {
            "from_id": "Gsnp_rs112305311",
            "to_id": "GENSG00000227232",
            "type": "association:SNP:gene",
            "source": "GTEx",
            "name": "WASH7P Expression",
            "info": {
                "biosample": "prostate",
                "p-value": 0.00617042,
                "true_df": 95.584,
                "pval_nominal": 0.00329429,
                "pval_nominal_threshold": 6.8408e-05,
                "slope": 0.859833,
                "log2_aFC": 1.053842,
                "log2_aFC_lower": 0.880039,
                "log2_aFC_upper": 1.257119
            },
            "_id": "E52d8b81e20847f297295ba39b1a97cbc47dde8c2c401f932c034613598876334"
        },


        """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node = {"_id": 'I'+DATA_SOURCE_GTEX, "type": "dataSource", 'name': DATA_SOURCE_GTEX, "source": DATA_SOURCE_GTEX}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        known_edge_ids = set()
        # these might be later used to patch existing documents in the database
        self.parsed_snp_ids = set()
        self.parsed_gene_ids = set()
        # the eQTL data entries does not provide any useful information about the SNPs, so we will not add GenomeNodes
        for d in self.eqtls:
            # create EdgeNode
            from_id = 'Gsnp_' + d['rs_id_dbSNP147_GRCh37p13']
            self.parsed_snp_ids.add(from_id)
            to_id = 'G' + d['gene_id'].split('.',1)[0]
            self.parsed_gene_ids.add(to_id)
            edge = {
                    'from_id': from_id, 'to_id': to_id,
                    'type': 'association:SNP:gene',
                    'source': DATA_SOURCE_GTEX,
                    'name': d['gene_name'] + ' Expression',
                    'info': {
                        'p-value': float(d['pval_true_df']),
                        'true_df': float(d['true_df']),
                        'pval_nominal': float(d['pval_nominal']),
                        'pval_nominal_threshold': float(d['pval_nominal_threshold']),
                        'slope': float(d['slope']),
                        'log2_aFC': float(d['log2_aFC']),
                        'log2_aFC_lower': float(d['log2_aFC_lower']),
                        'log2_aFC_upper': float(d.get('log2_aFC_upper', 'nan')),
                    }
            }
            if extra_info is not None:
                edge['info'].update(extra_info)
            edge['_id'] = 'E'+self.hash(str(edge))
            if edge['_id'] not in known_edge_ids:
                known_edge_ids.add(edge['_id'])
                edges.append(edge)
            if self.verbose and len(edges) % 100000 == 0:
                print("%d varients parsed" % len(edges), end='\r')
        return genome_nodes, info_nodes, edges

class EQTLParser_exSNP(EQTLParser):
    def get_mongo_nodes(self):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.

        Returns
        -------
        mongonodes: tuple
            The return tuple is (genome_nodes, info_nodes, edges)
            Each of the three is a list of multiple dictionaries, which contains the parsed data.

        References
        ----------
        http://www.exsnp.org/eQTL

        Notes
        -----
        1. The eQTL data set contains minimal information for the SNPs and Genes. Because of the lack of essential information like location,
        we will not create any GenomeNodes.
        2. The InfoNodes generated here have only one entry, with the type dataSource. The _id field should have a string value starts with "I", like "IeQTL".
        3. The Edges contains all the information of the eQTL dataset. All Edges here should have _id starts with "E". The "from_id" and "to_id" field are derived
        directly from the "exSNP" and "exGENEID" keys of the dataset.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = EQTLParser_exSNP('eQTL.txt')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        No GenomeNodes are generated:

        >>> print(genome_nodes)
        []

        One InfoNode with type dataSource is generated:

        >>> print(info_nodes[0])
        {
            "_id": "IeQTL",
            "type": "dataSource",
            "name": "eQTL",
            "source": "eQTL",
            "info": {
                "filename": "test_eQTL.txt"
            }
        }

        Example of an Edge:

        >>> print(edges[0])
        {
            "from_id": "Gsnp_rs945418",
            "to_id": "Ggeneid_1187",
            "from_type": "SNP",
            "to_type": "gene",
            "type": "association",
            "source": "eQTL",
            "name": "eQTL",
            "info": {
                "High_confidence": "N",
                "Population": "CEU",
                "CellType": "LCL",
                "DataSet": "EGEUV_EUR",
                "StudySet": "EGEUV",
                "SameSet": "0",
                "DiffSet": "1",
                "TotalSet": "1"
            },
            "_id": "Eee2bbe99af7e0bf29784974288db46b35d1e2d69c6c066c83a9a092f3716d47c"
        }

        """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node = {"_id": 'I'+DATA_SOURCE_EXSNP, "type": "dataSource", 'name': DATA_SOURCE_EXSNP, "source": DATA_SOURCE_EXSNP}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        known_edge_ids = set()
        # the eQTL data entries does not provide any useful information about the SNPs, so we will not add GenomeNodes
        for d in self.eqtls:
            # create EdgeNode
            from_id = 'Gsnp_rs'+d['exSNP']
            to_id = 'Ggeneid_'+d['exGENEID']
            edge = {
                'from_id': from_id, 'to_id': to_id,
                'type': 'association:SNP:gene',
                'source': DATA_SOURCE_EXSNP,
                'name': d['exGENE'] + ' Expression',
                'info': dict()
            }
            for k,v in d.items():
                if k not in ('exSNP', 'exGENEID', 'exGENE'):
                    edge['info'][k] = v
            edge['_id'] = 'E'+self.hash(str(edge))
            if edge['_id'] not in known_edge_ids:
                known_edge_ids.add(edge['_id'])
                edges.append(edge)
            if self.verbose and len(edges) % 100000 == 0:
                print("%d varients parsed" % len(edges), end='\r')
        return genome_nodes, info_nodes, edges
