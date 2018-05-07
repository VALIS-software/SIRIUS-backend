from sirius.parsers.Parser import Parser
from sirius.helpers.constants import DATA_SOURCE_GENOME

class GFFParser(Parser):
    """
    Parser for the GFF3 data format.

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
    parse_chunk
    parse_one_line_data
    get_mongo_nodes
    * inherited from parent class *
    jsondata
    save_json
    load_json
    save_mongo_nodes

    Notes
    -----
    The parsing of the file is generally done in 2 steps.
    First, calling the self.parse() method will open the input file, read each line, and put parsed data into self.data.
    The method self.save_json() from parent Parser class can be used to save self.data into a file with json format.
    Second, to generate the internal data structure for MongoDB, we further parse the self.data into three types of Mongo nodes.
    These are GenomeNodes, InfoNodes and Edges.
    After getting these nodes, we can directly upload them to our MongoDB.

    References
    ----------
    https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md

    Examples
    --------
    Initiate a GFFParser:

    >>> parser = GFFParser("GRCh38.latest.gff")

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
    def features(self):
        return self.data['features']

    @features.setter
    def features(self, value):
        self.data['features'] = value

    def parse(self):
        """
        Parse the GFF3 data format.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The comment line starting with a "##" or a "#!" will be parsed as metadata.
        3. After parsing, self.data['features'] will be a list of dictionaries containing the data.

        References
        ----------
        https://github.com/The-Sequence-Ontology/Specifications/blob/master/gff3.md

        Examples
        --------
        Initialize and parse the file:

        >>> parser = GFFParser('GRCH38.latest.gff')
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.features:

        >>> print(parser.features[0])
        {
            "seqid": "NC_000001.11",
            "source": "BestRefSeq",
            "type": "gene",
            "start": 11874,
            "end": 14409,
            "score": ".",
            "strand": "+",
            "phase": ".",
            "attributes": {
                "ID": "gene0",
                "Dbxref": "GeneID:100287102,HGNC:HGNC:37102",
                "Name": "DDX11L1",
                "description": "DEAD/H-box helicase 11 like 1",
                "gbkey": "Gene",
                "gene": "DDX11L1",
                "gene_biotype": "misc_RNA",
                "pseudo": "true"
            }
        }

        """
        # restart the reading of the file from the beginning
        self.filehandle.seek(0)
        assert self.parse_chunk(size=-1) == True, 'self.parse_chunk() did not finish parsing the entire file.'

    def parse_chunk(self, size=100000):
        """
        Parse the file line by line and stop when accumulated {size} number of features data.
        This method is useful when parsing very large data files, because it will have a limited memory usage.

        Parameters
        ----------
        size: int, optional
            The size of data to parse. Default size is 100000.

        Returns
        -------
        finished: bool
            Return True if the parsing hits the end of file, otherwise False.

        Notes
        -----
        1. This method uses self.filehandle and start reading from the current position in file.
        2. Recursivedly calling this method will continue to read the same file, return False when accumulated {size} number of data.
        3. If the end of file is reached, this method will return True.
        4. Recursivedly calling this method will not overwrite self.metadata, but will clean and overwrite self.features with the newly parsed data.

        Examples
        --------
        Initialize parser:

        >>> parser = GFFParser('GRCH38.latest.gff')

        Parse the file and stop at 10 features.

        >>> parser.parse_chunk(size=10)

        The self.features list should now contain 10 data.

        >>> print(len(parser.features)
        10

        This is usually done in a recurrsive manner:

        >>> while True:
        >>>     finished = parser.parse_chunk(size=10)
        >>>     MongoNodes = parser.get_mongo_nodes()
        >>>     (do sth, like upload_mongo_nodes)
        >>>     if finished == True:
        >>>         break

        Pass the argument size = -1 will cause this method to read until the end of the file:

        >>> finished = parser.parse_chunk(size=-1)
        >>> print(finished)
        True

        """
        self.features = []
        for line in self.filehandle:
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#' or line[1] == '!':
                    ls = line[2:].split(maxsplit=1)
                    if len(ls) == 2:
                        self.metadata[ls[0]] = ls[1]
                    elif len(ls) == 1:
                        self.metadata[ls[0]] = None
            elif line:
                d = self.parse_one_line_data(line)
                self.features.append(d)
                if self.verbose and len(self.features) % 100000 == 0:
                    print("%d data parsed" % len(self.features), end='\r')
                if len(self.features) == size:
                    if self.verbose:
                        print(f"Parsing file {self.filename} finished for chunk of size {size}" )
                    break
        else:
            if self.verbose:
                print(f"Parsing the entire file {self.filename} finished.")
            return True
        return False

    def parse_one_line_data(self, line):
        """
        Parse one line if the gff file into a dictionary.
        This method is usually called by self.parse() internally.

        Parameters
        ----------
        line: string
            The line of the file, which should be "\t" splitted and have 9 fields.

        Returns
        -------
        d: dictionary
            The dictionary contains the data for this line.

        """
        ls = line.split('\t')
        assert len(ls) == 9, "Error parsing this line:\n%s with %s split" % (line, len(ls))
        d = dict()
        d['seqid'], d['source'], d['type'] = ls[0:3]
        d['start'], d['end'] = int(ls[3]), int(ls[4])
        d['score'], d['strand'], d['phase'] = ls[5:8]
        d['attributes'] = dict()
        for attr in ls[8].split(';'):
            key, value = attr.split('=', maxsplit=1)
            d['attributes'][key] = value
        return d

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
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.features,
        which are contents of self.data
        2. The parsing of the gff file should be sequential, because the contig is determined by the {type: region} entries.
        If the seqid is standard like "NC_000001.10", the contig will be normalized to chr1
        Otherwise the contig will be the seqid, like "NT_187636.1"
        3. The unique _id for genes will be generated like {"_id": "Ggeneid_100287102"}, which used the GeneID.
        Duplicate entries with the same GeneID may be encountered, and they will be ignored for now.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = GFFParser('GRCH38.latest.gff')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        An example of the genome_nodes would be like:

        >>> print(genome_nodes[0])
        {
            "source": "GRCh38_gff",
            "type": "gene",
            "contig": "chr1",
            "start": 11874,
            "end": 14409,
            "length": 2536,
            "name": "DDX11L1",
            "info": {
                "ID": "gene0",
                "description": "DEAD/H-box helicase 11 like 1",
                "gbkey": "Gene",
                "gene": "DDX11L1",
                "gene_biotype": "misc_RNA",
                "pseudo": "true",
                "source": "BestRefSeq",
                "score": ".",
                "strand": "+",
                "phase": ".",
                "GeneID": "100287102",
                "HGNC": "HGNC:37102"
            },
            "_id": "Ggeneid_100287102"
        },

        Here the ['info'] dictionary contains optional data, and everything else is mandatory for the GenomeNode.

        The first info_node will be the dataSource:

        >>> print(into_nodes[0])
        {
            "_id": "IGRCh38_gff",
            "type": "dataSource",
            "name": "GRCh38_gff",
            "source": "GRCh38_gff",
            "info": {
                "filename": "test.gff",
                "gff-version": "3",
                "gff-spec-version": "1.21",
                "processor": "NCBI annotwriter",
                "genome-build": "GRCh38.p10",
                "genome-build-accession": "NCBI_Assembly:GCF_000001405.36",
                "annotation-date": None,
                "annotation-source": None,
                "sequence-region": "NC_000001.11 1 248956422",
                "species": "https://www.ncbi.nlm.nih.gov/Taxonomy/Browser/wwwtax.cgi?id=9606"
            }
        }

        The rest of the info_nodes are type "contig", which contains the informatino for contigs:

        >>> print(into_nodes[1])
        {
            "_id": "Icontigchr1",
            "type": "contig",
            "name": "chr1",
            "source": "GRCh38_gff",
            "info": {
                "ID": "id0",
                "Dbxref": "taxon:9606",
                "Name": "1",
                "chromosome": "1",
                "gbkey": "Src",
                "genome": "chromosome",
                "mol_type": "genomic DNA",
                "length": 248956422,
                "source": "RefSeq",
                "score": ".",
                "strand": "+",
                "phase": "."
            }
        }

        The edges should be empty:

        >>> print(edges)
        []

        """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node = {"_id": 'I'+DATA_SOURCE_GENOME, "type": "dataSource", "name": DATA_SOURCE_GENOME, "source": DATA_SOURCE_GENOME}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        # the seqid_contig dictionary is used to store the normalized names of contigs for each seqid
        if not hasattr(self, 'seqid_contig'): self.seqid_contig = dict()
        if not hasattr(self, 'known_id_set'): self.known_id_set = set()
        for feature in self.data['features']:
            d = feature.copy()
            ft = d.pop('type')
            seqid = d.pop('seqid')
            if ft == 'region':
                # the regions will be parsed into info nodes
                if d.pop('start') != 1:
                    raise RuntimeError(f"region should have start:1\n{d}")
                chromid = d['attributes'].get('chromosome', None)
                # We use names like chr1 to normalize the contig of normal seqids like NC_000001.10
                chrom_name = ('chr'+chromid) if chromid != None else 'Unknown'
                if seqid.startswith("NC_000") and chrom_name != 'Unknown':
                    contig_name = chrom_name
                else:
                    contig_name = seqid
                contig_info_node = {
                    '_id': 'Icontig' + contig_name,
                    'type': 'contig',
                    'name': contig_name,
                    'source': DATA_SOURCE_GENOME,
                    'info': d.pop('attributes')

                }
                contig_info_node['info']['length'] = d.pop('end')
                contig_info_node['info'].update(d)
                info_nodes.append(contig_info_node)
                self.seqid_contig[seqid] = contig_name
            else:
                # this seqid should have been seen earlier in this file
                contig_name = self.seqid_contig[seqid]
                # create gnode document with basic information
                start, end = d.pop('start'), d.pop('end')
                attributes = d.pop('attributes')
                gnode = {
                    'source': DATA_SOURCE_GENOME,
                    'type': ft,
                    'contig': contig_name,
                    'start': start,
                    'end': end,
                    'length': end - start + 1,
                    'name': attributes.pop('Name', ft),
                    'info': attributes
                }
                # add additional information to gnode['info']
                gnode['info'].update(d)
                # get geneID fron Dbxref, e.g GeneID:100287102,Genbank:NR_046018.2,HGNC:HGNC:37102
                dbxref = gnode['info'].pop('Dbxref', None)
                if dbxref != None:
                    for ref in dbxref.split(','):
                        refname, ref_id = ref.split(':', 1)
                        gnode['info'][refname] = ref_id
                        # use GeneID as the ID for this gene
                        if refname == 'GeneID' and gnode['type'] == 'gene':
                            gnode['_id'] = 'Ggeneid_' + ref_id
                if '_id' not in gnode:
                    gnode['_id'] = 'G' + ft + '_' + self.hash(str(gnode))
                if gnode['_id'] in self.known_id_set:
                    print("Warning, gnode with _id %s already exists!" % gnode['_id'])
                else:
                    self.known_id_set.add(gnode['_id'])
                    genome_nodes.append(gnode)
                if self.verbose and len(genome_nodes) % 100000 == 0:
                    print("%d GenomeNodes prepared" % len(genome_nodes), end='\r')
        if self.verbose:
            print("Parsing GFF into mongo nodes finished.")
        return genome_nodes, info_nodes, edges
