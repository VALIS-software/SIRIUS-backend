from sirius.parsers.Parser import Parser
from sirius.realdata.constants import DATA_SOURCE_EQTL

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

    References
    ----------
    http://www.exsnp.org/eQTL

    Examples
    --------
    Initiate a GWASParser:

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

        >>> parser = EQTLParser('eQTL.txt')
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.eqtls:

        >>> parser.eqtls[0]
        {
            "exSNP": "945418",
            "exGENEID": "1187",
            "exGENE": "CLCNKA",
            "High_confidence": "N",
            "Population": "CEU",
            "CellType": "LCL",
            "DataSet": "EGEUV_EUR",
            "StudySet": "EGEUV",
            "SameSet": "0",
            "DiffSet": "1",
            "TotalSet": "1"
        }

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
        1. The eQTL data set contains minimal information for the SNPs and Genes. Because of the lack of essential information like location,
        we will not create any GenomeNodes.
        2. The InfoNodes generated here have only one entry, with the type dataSource. The _id field should have a string value starts with "I", like "IeQTL".
        3. The Edges contains all the information of the eQTL dataset. All Edges here should have _id starts with "E". The "from_id" and "to_id" field are derived
        directly from the "exSNP" and "exGENEID" keys of the dataset.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = EQTLParser('eQTL.txt')
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
        info_node = {"_id": 'I'+DATA_SOURCE_EQTL, "type": "dataSource", 'name': DATA_SOURCE_EQTL, "source": DATA_SOURCE_EQTL}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        known_edge_ids = set()
        # the eQTL data entries does not provide any useful information about the SNPs, so we will not add GenomeNodes
        if 'reference' in self.metadata:
            assembly = self.metadata['reference']
        else:
            assembly = 'GRCh38'
        for d in self.eqtls:
            # create EdgeNode
            from_id = 'Gsnp_rs'+d['exSNP']
            to_id = 'Ggeneid_'+d['exGENEID']
            edge = {'from_id': from_id, 'to_id': to_id,
                    'from_type': 'SNP', 'to_type': 'gene',
                    'type': 'association',
                    'source': DATA_SOURCE_EQTL,
                    'name': DATA_SOURCE_EQTL,
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
