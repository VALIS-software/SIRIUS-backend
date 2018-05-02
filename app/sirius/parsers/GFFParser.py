#!/usr/bin/env python

import os, sys, json, gzip
from sirius.parsers.Parser import Parser
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_GENOME

class GFFParser(Parser):
    """
    Parser for the GFF3 data format.

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
        1. This method will open self.filename as .gz compressed files if the self.filename has the extension of .gz,
        otherwise will open self.filename as text file for reading.
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
        metadata = {'filename': self.filename}
        features = []
        if self.ext == '.gz':
            filehandle = gzip.open(self.filename, 'rt')
        else:
            filehandle = open(self.filename)
        for line in filehandle:
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#' or line[1] == '!':
                    ls = line[2:].split(maxsplit=1)
                    if len(ls) == 2:
                        metadata[ls[0]] = ls[1]
                    elif len(ls) == 1:
                        metadata[ls[0]] = None
            elif line:
                d = self.parse_one_line_data(line)
                features.append(d)
                if self.verbose and len(features) % 100000 == 0:
                    print("%d data parsed" % len(features), end='\r')
        filehandle.close()
        if self.verbose:
            print("Parsing GFF data finished.")
        self.metadata.update(metadata)
        self.data['features'] = features

    def parse_save_data_in_chunks(self, file_prefix='dataChunk', chunk_size=100000):
        """ Specializd function to parse and safe data in chunks to reduce memory usage """
        metadata = {'filename': self.filename}
        features = []
        i_chunk = 0
        out_filenames = []
        if self.ext == '.gz':
            filehandle = gzip.open(self.filename, 'rt')
        else:
            filehandle = open(self.filename)
        for line in filehandle:
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#' or line[1] == '!':
                    ls = line[2:].split(maxsplit=1)
                    if len(ls) == 2:
                        metadata[ls[0]] = ls[1]
                    elif len(ls) == 1:
                        metadata[ls[0]] = None
            elif line:
                d = self.parse_one_line_data(line)
                features.append(d)
                if len(features) == chunk_size:
                    filename = file_prefix + "_%04d.json" % i_chunk
                    chunk_data = {'metadata': metadata, 'features': features}
                    with open(filename, 'w') as out:
                        json.dump(chunk_data, out, indent=2)
                    out_filenames.append(filename)
                    if self.verbose:
                        print("%s parsed and saved" % filename)
                    features = []
                    i_chunk += 1
        filehandle.close()
        # add the lask chunk
        if len(features) > 0:
            filename = file_prefix + "_%04d.json" % i_chunk
            chunk_data = {'metadata': metadata, 'features': features}
            with open(filename, 'w') as out:
                json.dump(chunk_data, out, indent=2)
            out_filenames.append(filename)
            if self.verbose:
                print("%s parsed and saved" % filename)
        return out_filenames

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
            The results of this function will be stored in self.mongonodes for cache.

        Notes
        -----
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.features,
        which are contents of self.data
        2. The parsing of the gff file should be sequential, because the data entries only have `seqid` but not `chromid`.
        The chromid which stands for chromosome id, needs to be determined by the {type: region} entries.
        Those entries will have data like {"attributes.chromosome": "1"}.
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
            "assembly": "GRCh38",
            "source": "GRCh38_gff",
            "type": "region",
            "chromid": 1,
            "start": 1,
            "end": 248956422,
            "length": 248956422,
            "name": "1",
            "info": {
                "ID": "id0",
                "chromosome": "1",
                "gbkey": "Src",
                "genome": "chromosome",
                "mol_type": "genomic DNA",
                "source": "RefSeq",
                "score": ".",
                "strand": "+",
                "phase": ".",
                "taxon": "9606"
            },
            "_id": "Gregion_217b4de5549598efcb64a4897f85044a7606d9018443a688f120788ebc020bc5"
        },

        Here the ['info'] dictionary contains optional data, and everything else is mandatory for the GenomeNode.

        The info_nodes will have only one element for the dataSource:

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

        The edges should be empty:

        >>> print(edges)
        []

        """
        if hasattr(self, 'mongonodes'): return self.mongonodes
        genome_nodes, info_nodes, edge_nodes = [], [], []
        # add dataSource into InfoNodes
        info_node = {"_id": 'I'+DATA_SOURCE_GENOME, "type": "dataSource", "name": DATA_SOURCE_GENOME, "source": DATA_SOURCE_GENOME}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        # add data as GenomeNodes
        if 'assembly' in self.metadata:
            assembly = self.metadata['assembly']
        else:
            assembly = 'GRCh38'
        if not hasattr(self, 'seqid_loc'): self.seqid_loc = dict()
        if not hasattr(self, 'known_id_set'): self.known_id_set = set()
        for feature in self.data['features']:
            d = feature.copy()
            ft = d.pop('type')
            # we are skipping the contigs for now, since we don't know where they are
            if not d['seqid'].startswith("NC_000"): continue
            if ft == 'region':
                if 'chromosome' in d['attributes']:
                    try:
                        self.seqid_loc[d['seqid']] = chromo_idxs[d['attributes']['chromosome']]
                    except:
                        self.seqid_loc[d['seqid']] = None
            # create gnode document with basic information
            gnode = {'assembly': assembly, "source": DATA_SOURCE_GENOME, 'type': ft}
            try:
                gnode['chromid'] = self.seqid_loc[d.pop('seqid')]
                gnode['start'] = d.pop('start')
                gnode['end'] = d.pop('end')
                gnode['length'] = gnode['end'] - gnode['start'] + 1
            except KeyError:
                # we ignore those genes with unknown locations for now
                continue
            try:
                gnode['name'] = d['attributes'].pop('Name')
            except KeyError:
                # we use type as name
                gnode['name'] = ft
            # add additional information to gnode['info']
            gnode['info'] = d.pop('attributes')
            gnode['info'].update(d)
            # get geneID fron Dbxref, e.g GeneID:100287102,Genbank:NR_046018.2,HGNC:HGNC:37102
            try:
                dbxref = gnode['info'].pop('Dbxref')
                for ref in dbxref.split(','):
                    refname, ref_id = ref.split(':', 1)
                    gnode['info'][refname] = ref_id
                    # use GeneID as the ID for this gene
                    if refname == 'GeneID' and gnode['type'] == 'gene':
                        gnode['_id'] = 'Ggeneid_' + ref_id
            except KeyError:
                pass
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
        self.mongonodes = (genome_nodes, info_nodes, edge_nodes)
        return self.mongonodes
