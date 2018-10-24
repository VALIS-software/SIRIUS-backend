from sirius.helpers.constants import CHROMO_IDXS, ENCODE_COLOR_TYPES, KNOWN_CONTIGS
from sirius.helpers.constants import DATA_SOURCE_ENCODE, DATA_SOURCE_ROADMAP_EPIGENOMICS
from sirius.parsers.parser import Parser
from sirius.parsers.liftover import lo

class BEDParser(Parser):
    """
    Parser for the .bed data formats.

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
    * inherited from parent class *
    jsondata
    save_json
    load_json
    save_mongo_nodes
    hash

    Notes
    -----
    The parsing of the file is generally done in 2 steps.
    1. calling the self.parse() method will open the input file, read each line, and put parsed data into self.data.
    The method self.save_json() from parent Parser class can be used to save self.data into a file with json format.
    The parse() method is defined in the VCFParser class.
    2. To generate the internal data structure for MongoDB, we further parse the self.data into three types of Mongo nodes.
    These are GenomeNodes, InfoNodes and Edges.
    The get_mongo_nodes() method should be defined in subclasses to match the usage of individual datasets.

    References
    ----------
    https://genome.ucsc.edu/FAQ/FAQformat.html#format1

    Examples
    --------
    Initiate a BEDParser:

    >>> parser = BEDParser("ENCODE.bed.gz")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    """

    @property
    def intervals(self):
        return self.data['intervals']

    @intervals.setter
    def intervals(self, value):
        self.data['intervals'] = value

    def parse(self):
        """
        Parse the bed data format.

        The file in BED format is text file with tab-separations.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The bed file contains no metadata.
        3. The bed file contains no comment line.
        4. Each line in the bed file have between 3 and 12 columns. The column labels are fixed to
            ['chrom', 'start', 'end', 'name', 'score', 'strand', 'thickStart', 'thickEnd', 'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
        5. After parsing, self.data["intervals"] will be a list of dictionaries containing the data.

        References
        ----------
        https://genome.ucsc.edu/FAQ/FAQformat.html#format1

        Examples
        --------
        Initialize and parse the file:

        >>> parser = GFFParser('ENCODE.bed.gz')
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.variants:

        >>> print(parser.intervals[0])
        {
            "chrom": "chr1",
            "start": "10244",
            "end": "10357",
            "name": "EH37E1055273",
            "score": "0",
            "strand": ".",
            "thickStart": "10244",
            "thickEnd": "10357",
            "itemRgb": "6,218,147"
        }

        """
        self.filehandle.seek(0)
        assert self.parse_chunk(size=-1) == True, 'self.parse_chunk() did not finish parsing the entire file.'

    def parse_chunk(self, size=100000):
        """
        Parse the file line by line and stop when accumulated {size} number of features data.
        """
        bed_labels = ['chrom', 'start', 'end', 'name', 'score', 'strand', 'thickStart', 'thickEnd', 'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']
        self.intervals = []
        chr_name_id = dict(('chr'+s, i) for s,i in CHROMO_IDXS.items())
        for line in self.filehandle:
            ls = line.strip().split('\t') # remove '\n'
            if ls[0] in chr_name_id:
                self.intervals.append(dict([*zip(bed_labels, ls)]))
                if len(self.intervals) == size:
                    if self.verbose:
                        print(f"Parsing file {self.filename} finished for chunk of size {size}" )
                    break
        else:
            if self.verbose:
                print(f"Parsing the entire file {self.filename} finished.")
            return True
        return False

    def get_mongo_nodes(self):
        """ general bed file parsing into genomenodes """
        genome_nodes, info_nodes, edges = [], [], []
        data_source = self.metadata.get('source', self.filename)
        for d in self.data['intervals']:
            start = int(d['start']) + 1
            end = int(d['end'])
            gnode = {
                'source': data_source,
                'type': 'interval',
                'name': d.get('name', 'Unlabeled'),
                'contig': d['chrom'],
                'start': start,
                'end': end,
                'length': end - start + 1,
                'info': {
                }
            }
            for k in ['score', 'strand', 'thickStart', 'thickEnd', 'itemRgb', 'blockCount', 'blockSizes', 'blockStarts']:
                if k in d:
                    gnode['info'][k] = d[k]
            gnode['_id'] = 'G' + '_' + self.hash(str(gnode))
            genome_nodes.append(gnode)
        return genome_nodes, info_nodes, edges

class BEDParser_ENCODE(BEDParser):
    def get_mongo_nodes(self, liftover=False):
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
        2. Since the bed files does not contain metadata, some keys in the metadata are required to be set before calling this function.
        These are: "biosample", "accession", "description", "targets". All of these information can be obtained by the web api.
        3. GenomeNodes generated will be internals, with very basic information like "info.biosample", "info.accession" and "info.targets".
        4. All GenomeNodes should have _id starting with "G", like "G_8e2b44c80d0562..".
        4. The type of the intervals will be one of the below, based on their color codes, defined in sirius.helpers.constants.ENCODE_COLOR_TYPES
            (255,0,0): 'Promoter-like',
            (255,205,0): 'Enhancer-like',
            (0,176,240): 'CTCF-only',
            (6,218,147): 'DNase-only',
            (225,225,225): 'Inactive',
            (140,140,140): 'Unclassified'
        5. To reduce the total number of GenomeNodes in the MongoDB, we chose to ignore the ones with type "Inactive" and "Unclassified".
        This reduces the total number of GenomeNodes by a factor of 10.
        6. One InfoNode will be generated, with type "ENCODE_accession". This infonode stores the mettadata for this accession, which is useful for
        searching distinct values of certain keys.
        7. No edges are created in this parsing.

        To-Do
        -----
        The ENCODE dataset is under assembly GRCh37 but our browser is working under GRCh38, a coordinate conversion is needed.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = BEDParser_ENCODE('ENCODE.bed.gz')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        An example of the genome_nodes would be like:

        >>> print(genome_nodes[0])
        {
            "source": "ENCODE",
            "type": "   ,
            "name": "EH37E1055273",
            "contig": "chr1",
            "start": 10244,
            "end": 10357,
            "length": 114,
            "info": {
                "score": "0",
                "strand": ".",
                "thickStart": "10244",
                "thickEnd": "10357",
                "biosample": "#biosample#",
                "accession": "#accession#",
                "description": "#description#",
                "targets": ["#Target#"]
            },
            "_id": "G_bfa181eb46e5f39606825269b63d65442b21d1bf0d8b114e5423dbeadb9ea88c"
        },

        The info_nodes will have only one element with type "ENCODE_accession":
        (The data with ## should be set in self.metadata by the user)

        >>> print(info_nodes[0])
        {
            "_id": "I_#accession#",
            "type": "ENCODE_accession",
            "name": "#accession#",
            "source": "ENCODE",
            "info": {
                "filename": "test.bed",
                "biosample": "#biosample#",
                "accession": "#accession#",
                "description": "#description#",
                "targets": [#target#],
                "types": [
                    "DNase-only"
                ]
            }
        }

        No edges are created:

        >>> print(edges)
        []

        """
        # these metadata should be set by downloading script for ENCODE data
        biosample = self.metadata['biosample']
        accession = self.metadata['accession']
        description = self.metadata['description']
        targets = self.metadata['targets']
        # start parsing
        genome_nodes, info_nodes, edges = [], [], []
        # add data as GenomeNodes
        all_types = set()
        for interval in self.data['intervals']:
            d = interval.copy()
            color = tuple(int(c) for c in d.pop('itemRgb').split(','))
            tp = ENCODE_COLOR_TYPES[color]
            # We skip the two "not-useful types" for now
            if tp == 'Inactive' or tp == 'Unclassified':
                continue
            all_types.add(tp) # keep track of the types for this data file
            name = d.pop('name')
            contig = d.pop('chrom')
            start, end = int(d.pop('start')), int(d.pop('end'))
            length = end - start
            # liftover using pyliftover
            if liftover is True:
                strand = d.get('strand', '.')
                lo_result = lo.convert_coordinate(contig, start, strand)
                if len(lo_result) > 0:
                    # here we replace contig and position, but leave the others unchanged
                    contig, start, target_strand, conversion_chain_score = lo_result[0]
                else:
                    if self.verbose:
                        print(f"Warning! liftover failed for {interval}, skipping")
                    continue
                end = start + length - 1
            # skip unknown contigs
            if contig not in KNOWN_CONTIGS:
                continue
            # we change 0-based pos to 1-based
            gnode = {
                'source': DATA_SOURCE_ENCODE,
                'type': tp,
                'name': name,
                'contig': contig,
                'start': start+1,
                'end':end,
                'length': length,
            }
            gnode['info'] = d
            gnode['info'].update({
                'biosample': biosample,
                'accession': accession,
                'description': description,
                'targets': targets,
            })
            gnode['_id'] = 'G' + '_' + self.hash(str(gnode))
            genome_nodes.append(gnode)
            if self.verbose and len(genome_nodes) % 100000 == 0:
                print("%d GenomeNodes prepared" % len(genome_nodes), end='\r')
        # add ENCODE_accession into InfoNodes
        info_node = {"_id": 'I_'+accession, "type": "ENCODE_accession", "name": accession, "source": DATA_SOURCE_ENCODE}
        info_node['info'] = self.metadata.copy()
        # store all available types in the InfoNode
        info_node['info']['types'] = sorted(all_types)
        info_nodes.append(info_node)
        if self.verbose:
            print("Parsing BED into mongo nodes finished.")
        return genome_nodes, info_nodes, edges

class BEDParser_ROADMAP_EPIGENOMICS(BEDParser):
    """
    Parser for Roadmap Epigenomics data
    Reference from https://github.com/ryanlayer/giggle/blob/master/examples/rme/states.txt
    1_TssA	Active_TSS
    2_TssAFlnk	Flanking_Active_TSS
    3_TxFlnk	Transcr_at_gene_5_and_3
    4_Tx	Strong_transcription
    5_TxWk	Weak_transcription
    6_EnhG	Genic_enhancers
    7_Enh	Enhancers
    8_ZNF/Rpts	ZNF_genes_and_repeats
    9_Het	Heterochromatin
    10_TssBiv	Bivalent_Poised_TSS
    11_BivFlnk	Flanking_Bivalent_TSS_Enh
    12_EnhBiv	Bivalent_Enhancer
    13_ReprPC	Repressed_PolyComb
    14_ReprPCWk	Weak_Repressed_PolyComb
    15_Quies	Quiescent_Low
    """
    roadmap_types = {
        'Active_TSS': 'TssA',
        'Flanking_Active_TSS': 'TssAFlnk',
        'Transcr_at_gene_5_and_3': 'TxFlnk',
        'Strong_transcription': 'Tx',
        'Weak_transcription': 'TxWk',
        'Genic_enhancers': 'EnhG',
        'Enhancers': 'Enh',
        'ZNF_genes_and_repeats': 'ZNF/Rpts',
        'Heterochromatin': 'Het',
        'Bivalent_Poised_TSS': 'TssBiv',
        'Flanking_Bivalent_TSS_Enh': 'BivFlnk',
        'Repressed_PolyComb': 'ReprPC',
        'Weak_Repressed_PolyComb': 'ReprPCWk',
        'Quiescent_Low': 'Quies'
    }
    def get_mongo_nodes(self):
        """ giggle roadmap bed file parsing into genomenodes
        ref: https://egg2.wustl.edu/roadmap/web_portal/imputed.html
        """
        genome_nodes, info_nodes, edges = [], [], []
        gtype, biosample = self.parse_file_name()
        for d in self.data['intervals']:
            start = int(d['start']) + 1
            end = int(d['end'])
            gnode = {
                'source': DATA_SOURCE_ROADMAP_EPIGENOMICS,
                'type': gtype,
                'name': d['name'],
                'contig': d['chrom'],
                'start': start,
                'end': end,
                'length': end - start + 1,
                'info': {
                    'filename': self.filename
                }
            }
            if biosample is not None:
                gnode['info']['biosample'] = biosample
            gnode['_id'] = 'G' + '_' + self.hash(str(gnode))
            genome_nodes.append(gnode)
        # store the type and info.biosample in a InfoNode of type "ROADMAP_ANNOTATION"
        info_nodes.append({
            '_id': f'I_roadmap_{gtype}_{biosample}',
            'name': self.filename,
            'type': 'ROADMAP_ANNOTATION',
            'source': DATA_SOURCE_ROADMAP_EPIGENOMICS,
            'info': {
                'filename': self.filename,
                'types': gtype,
                'biosample': biosample,
            }
        })
        return genome_nodes, info_nodes, edges

    def parse_file_name(self):
        """
        Parse the file name of the roadmap bedgz file into specific fields
        """
        namestr = self.filename.split('.')[0]
        gtype = 'interval'
        biosample = None
        for typename in self.roadmap_types.keys():
            if namestr.endswith(typename):
                gtype = typename
                biosample = namestr[-len(typename)+1:]
        return gtype, biosample
