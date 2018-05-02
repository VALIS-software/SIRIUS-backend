from sirius.parsers.Parser import Parser
from sirius.realdata.constants import CHROMO_IDXS, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP
import re

def str_to_type(s):
    """ Utility function to convert a type name to the actual type """
    s = s.strip().lower()
    if s == 'string' or s == 'str':
        return str
    elif s == 'float' or s == 'double':
        return float
    elif s == 'integer' or s == 'int':
        return int
    elif s == 'flag':
        return bool

class VCFParser(Parser):
    """
    Parser for the VCF data formats.

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
    http://www.internationalgenome.org/wiki/Analysis/vcf4.0/

    Examples
    --------
    Initiate a VCFParser:

    >>> parser = VCFParser("clinvar.vcf")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    """

    @property
    def variants(self):
        return self.data['variants']

    @variants.setter
    def variants(self, value):
        self.data['variants'] = value

    def parse(self):
        """
        Parse the vcf data format.

        The file in VCF format is text file with tab-separations.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The comment lines starting with "##" will be parsed as metadata.
        3. The comment line starting with a single "#" will be parsed as the label line.
        4. The metadata["INFO"] will store the type of each info keys, these types will be used when parsing the INFO block. The flags will be parsed as {"XX": True}
        5. After parsing, self.data["variants"] will be a list of dictionaries containing the data.

        References
        ----------
        http://www.internationalgenome.org/wiki/Analysis/vcf4.0/

        Examples
        --------
        Initialize and parse the file:

        >>> parser = VCFParser('ClinVar.vcf.gz')
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.variants:

        >>> print(parser.variants[0])
        {
            "CHROM": "1",
            "POS": "1014042",
            "ID": "475283",
            "REF": "G",
            "ALT": "A",
            "QUAL": ".",
            "FILTER": ".",
            "INFO": {
                "ALLELEID": 446939,
                "CLNDISDB": "MedGen:C4015293,OMIM:616126,Orphanet:ORPHA319563",
                "CLNDN": "Immunodeficiency_38_with_basal_ganglia_calcification",
                "CLNHGVS": "NC_000001.11:g.1014042G>A",
                "CLNREVSTAT": "criteria_provided,_single_submitter",
                "CLNSIG": "Benign",
                "CLNVC": "single_nucleotide_variant",
                "CLNVCSO": "SO:0001483",
                "GENEINFO": "ISG15:9636",
                "MC": "SO:0001583|missense_variant",
                "ORIGIN": "1",
                "RS": "143888043"
            }
        }

        """
        # restart the reading of the file from the beginning
        self.filehandle.seek(0)
        assert self.parse_chunk(size=-1) == True, '.parse_chunk() did not finish parsing the entire file.'

    def parse_chunk(self, size=1000000):
        """
        Parse the file line by line and stop when accumulated {size} number of variants data.
        This method is useful when parsing very large data files, because it will have a limited memory usage.

        Parameters
        ----------
        size: int, optional
            The size of each chunk to parse. Default size is 1,000,000.

        Returns
        -------
        finished: bool
            Return True if the parsing hits the end of file, otherwise False.

        Notes
        -----
        1. This method uses self.filehandle and start reading from the current position in file.
        2. Recursivedly calling this method will continue to read the same file, return False when accumulated {size} number of data.
        3. If the end of file is reached, this method will return True.
        4. Recursivedly calling this method will not overwrite self.metadata, but will clean and overwrite self.variants with the newly parsed data.

        Examples
        --------
        Initialize parser:

        >>> parser = VCFParser('ClinVar.vcf.gz')

        Parse the file and stop at 10 variants.

        >>> parser.parse_chunk(size=10)

        The self.variants list should now contain 10 data.

        >>> print(len(parser.variants)
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
        # initiate metadata dict
        if 'INFO' not in self.metadata:
            self.metadata['INFO'] = dict()
        # reset variants
        self.variants = []
        # for splitting the metadata info line
        pattern = re.compile(''',(?=(?:[^'"]|'[^']*'|"[^"]*")*$)''')
        for line in self.filehandle:
            line = line.strip() # remove '\n'
            if line[0] == '#':
                if line[1] == '#':
                    ls = line[2:].split('=', 1)
                    if len(ls) == 2:
                        if ls[0] == 'INFO':
                            fmtstr = ls[1].strip()
                            if fmtstr[0] == '<' and fmtstr[-1] == '>':
                                keyvalues = pattern.split(fmtstr[1:-1])
                                fmtdict = dict(kv.split('=',1) for kv in keyvalues)
                                name = fmtdict["ID"]
                                descpt = fmtdict["Description"]
                                # remove the \" in Description
                                if descpt[0] == '\"' and descpt[-1] == '\"':
                                    descpt = descpt[1:-1]
                                self.metadata['INFO'][name] = {'Type': fmtdict['Type'], "Description": descpt}
                        else:
                            self.metadata[ls[0]] = ls[1]
                else:
                    # title line
                    self.labels = line[1:].split()
            elif line:
                d = self.parse_one_line_data(line)
                self.variants.append(d)
                if self.verbose and len(self.variants) % 100000 == 0:
                    print("%d data parsed" % len(self.variants), end='\r')
                if len(self.variants) == size:
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
        Parse one line of VCF data.

        Parameters
        ----------
        line: string
            The single line in the file should be tab-separated.

        Returns
        -------
        d: dictionary
            The dictionary contains the data for this line

        Notes
        -----
        1. The INFO block will be further splitted by the ";".
        2. After splitting, if one phrase has an "=" sign, like "A=ss", it will be parsed into a {key:value} pair like {"A": "ss"} in the d["INFO"] dictionary.
        3. If one phrase is a flag, like "FL", it will be parsed into a {key: True} pair like "{"FL": True}" in the d["INFO"] dictionary.

        """
        ls = line.strip().split('\t')
        assert len(ls) == len(self.labels), "Error parsing this line:\n%s with %s split" % (line, len(ls))
        d = dict(zip(self.labels, ls))
        dinfo = dict()
        for keyandvalue in d['INFO'].split(';'):
            if '=' in keyandvalue:
                k, v = keyandvalue.split('=', 1)
                vtype = self.metadata['INFO'][k]['Type']
                v = str_to_type(vtype)(v)
            else:
                k = keyandvalue
                vtype = self.metadata['INFO'][k]['Type']
                if vtype.lower() != 'flag':
                    print('line\nWarning! No "=" found in the key %s, but its Type is not Flag' % k)
                v = True # set flag to true
            dinfo[k] = v
        d['INFO'] = dinfo
        return d


class VCFParser_ClinVar(VCFParser):
    """
    VCFParser_ClinVar is a subclass of VCFParser class
    It is specifically designed to convert the ClinVar data into MongoDB documents.

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
    get_mongo_nodes
    * inherited from parent class *
    parse
    parse_chunk
    parse_one_line_data
    jsondata
    save_json
    load_json
    save_mongo_nodes
    hash

    Examples
    --------
    Initiate a VCFParser_ClinVar:

    >>> parser = VCFParser_ClinVar("clinvar.vcf")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    Get the Mongo nodes

    >>> mongo_nodes = parser.get_mongo_nodes()

    Save the Mongo nodes to a file

    >>> parser.save_mongo_nodes('output.mongonodes')

    """
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
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.varients,
        which are contents of self.data
        2. The GenomeNodes generated from this parsing should be Varients. They should all have "_id" started with "G".
        3. If the variant is a SNP, the rs# will be used, like "Gsnp_rs4950928".
        4. If the variant is not a SNP, the content of the variant will be hashed to make the _id like "Gvariant_28120123ewe129301"
            Duplicated SNPs with the same _id are ignored.
        5. Each variant can be associated with multiple traits.
        6. The InfoNodes generated contain 1 dataSource and multiple traits. They should all have "_id" values start with "I", like "IGWAS", or "Itrait_79485.."
        7. The description of the trait is read from the "INFO.CLINDN" block, separated by "|", with number of traits matching the number identifiers in CLNDISDB.
        8. The _id for the traits are computed as a hash of the trait description in lower case. The "_" in the description is replaced by " ",
        to improve the text search in MongoDB, and match the descriptions of GWAS data set. Duplicated traits with the same _id are ignored.
        9. The Edges generated are connections between the variants and traits. Each edge should have an "_id" starts with "E", and "from_id" = the variant's _id,
        and "to_id" = the trait's _id. All edges have the type "association" for now. Details of the GWAS studies are put in to the 'info' block of the edges.
        The 'info.p-value' is set to 0 to enable the query with filters of maximum p-values.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = VCFParser_ClinVar('GWAS.tsv')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        The GenomeNodes contain the information for the SNPs:

        >>> print(genome_nodes[0])
        {
            "_id": "Gsnp_rs143888043",
            "assembly": "GRCh38",
            "chromid": 1,
            "start": 1014042,
            "end": 1014042,
            "length": 1,
            "source": "ClinVar",
            "name": "RS143888043",
            "type": "SNP",
            "info": {
                "variant_ref": "G",
                "variant_alt": "A",
                "filter": ".",
                "qual": ".",
                "ALLELEID": 446939,
                "CLNVCSO": "SO:0001483",
                "GENEINFO": "ISG15:9636",
                "MC": "SO:0001583|missense_variant",
                "ORIGIN": "1",
                "CLNHGVS": "NC_000001.11:g.1014042G>A"
            }
        }

        The first InfoNode is the dataSource:

        >>> print(info_nodes[0])
        {
            "_id": "IClinVar",
            "type": "dataSource",
            "name": "ClinVar",
            "source": "ClinVar",
            "info": {
                "filename": "test_clinvar.vcf",
                "INFO": {
                "AF_ESP": {
                    "Type": "Float",
                    "Description": "allele frequencies from GO-ESP"
                },
                "AF_EXAC": {
                    "Type": "Float",
                    "Description": "allele frequencies from ExAC"
                },
                "AF_TGP": {
                    "Type": "Float",
                    "Description": "allele frequencies from TGP"
                },
                "ALLELEID": {
                    "Type": "Integer",
                    "Description": "the ClinVar Allele ID"
                },
                "CLNDN": {
                    "Type": "String",
                    "Description": "ClinVar's preferred disease name for the concept specified by disease identifiers in CLNDISDB"
                },
                ...
            }
        }

        The rest of the InfoNodes are traits:

        >>> print(info_nodes[1])
        {
            "_id": "Itrait_19da58905fbaeeb465993befc5012168b7149467207cc3275b322413b224d632",
            "type": "trait",
            "name": "I3WBGC",
            "source": "ClinVar",
            "info": {
                "MedGen": "C4015293",
                "OMIM": "616126",
                "Orphanet": "ORPHA319563",
                "description": "Immunodeficiency 38 with basal ganglia calcification"
            }
        }

        The Edges contain some information specific to the study:

        >>> print(edges[0])
        {
            "from_id": "Gsnp_rs143888043",
            "to_id": "Itrait_19da58905fbaeeb465993befc5012168b7149467207cc3275b322413b224d632",
            "type": "association",
            "source": "ClinVar",
            "name": "ClinVar",
            "info": {
                "CLNREVSTAT": "criteria_provided,_single_submitter",
                "p-value": 0
            },
            "_id": "E43914e235ff17c98c065db27806ef05cb891905c8736ef7c51c02c33b021c62a"
        }

        """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node= {
            "_id": 'I'+DATA_SOURCE_CLINVAR,
            "type": "dataSource",
            'name':DATA_SOURCE_CLINVAR,
            "source": DATA_SOURCE_CLINVAR,
            'info': self.metadata.copy()
            }
        info_nodes.append(info_node)
        known_vid, known_traits, known_edge_ids = set(), set(), set()
        if 'reference' in self.metadata:
            assembly = self.metadata['reference']
        else:
            assembly = 'GRCh38'
        for d in self.variants:
            # we will abandon this entry if the CHROM is not recognized
            if d['CHROM'] not in CHROMO_IDXS: continue
            chromid = CHROMO_IDXS[d['CHROM']]
            # create GenomeNode for Varient
            if 'RS' in d['INFO']:
                rs = str(d["INFO"]["RS"])
                variant_id = "Gsnp_rs" + rs
                variant_type = "SNP"
                name = 'RS' + rs
            else:
                variant_type = d['INFO']['CLNVC'].lower()
                pos = str(d['POS'])
                v_ref, v_alt = d['REF'], d['ALT']
                variant_key_string = '_'.join([variant_type, str(chromid), pos, v_ref, v_alt])
                variant_id = 'Gvariant_' + self.hash(variant_key_string)
                name = ' '.join(s.capitalize() for s in variant_type.split('_'))
            pos = int(d['POS'])
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {
                    '_id': variant_id,
                    'assembly': assembly,
                    'chromid':chromid,
                    'start': pos,
                    'end': pos,
                    'length': 1,
                    'source': DATA_SOURCE_CLINVAR,
                    'name': name,
                    'type': variant_type
                }
                gnode['info'] = {
                    'variant_ref': d["REF"],
                    'variant_alt': d['ALT'],
                    'filter': d['FILTER'],
                    'qual': d['QUAL']
                }
                for key in ('ALLELEID', 'CLNVCSO', 'GENEINFO', 'MC', 'ORIGIN', 'CLNHGVS'):
                    try:
                        gnode['info'][key] = d["INFO"][key]
                    except:
                        pass
                genome_nodes.append(gnode)
            # we will abandon this entry if no trait information found
            if 'CLNDN' not in d['INFO'] or 'CLNDISDB' not in d['INFO']: continue
            # create InfoNode for trait, one entry could have multiple traits
            this_trait_ids = []
            trait_names = d['INFO']['CLNDN'].split('|')
            trait_CLNDISDBs = d['INFO']['CLNDISDB'].split('|')
            if len(trait_names) != len(trait_CLNDISDBs):
                print(f"Number of traits in in CLNDN and CLNDISDB not consistent! Skipping this data {d}")
                continue
            for trait_name, trait_disdb in zip(trait_names, trait_CLNDISDBs):
                trait_desp = trait_name.replace("_"," ")
                trait_id = 'Itrait_' + self.hash(trait_desp.lower())
                this_trait_ids.append(trait_id)
                short_name = (''.join(s[0] for s in trait_desp.split())).upper()
                if trait_id not in known_traits:
                    infonode = {
                        '_id': trait_id,
                        'type': 'trait',
                        'name': short_name,
                        'source': DATA_SOURCE_CLINVAR,
                        'info': dict()
                    }
                    for nameidx in trait_disdb.split(','):
                        if ':' in nameidx:
                            name, idx = nameidx.split(':', 1)
                            infonode['info'][name] = idx
                    # The info.description is where we search for trait
                    # use space here for text search in MongoDB
                    infonode['info']['description'] = trait_desp
                    info_nodes.append(infonode)
                    known_traits.add(trait_id)
            # create EdgeNode for each trait in this entry
            for trait_id in this_trait_ids:
                # add study to edges
                edge = {'from_id': variant_id , 'to_id': trait_id,
                        'type': 'association',
                        'source': DATA_SOURCE_CLINVAR,
                        'name': 'ClinVar',
                        'info': {
                            'CLNREVSTAT': d['INFO']["CLNREVSTAT"],
                            'p-value': 0,
                        }
                       }
                edge['_id'] = 'E'+self.hash(str(edge))
                if edge['_id'] not in known_edge_ids:
                    known_edge_ids.add(edge['_id'])
                    edges.append(edge)
                if self.verbose and len(edges) % 100000 == 0:
                    print("%d variants parsed" % len(edges), end='\r')
        if self.verbose:
            print("Parsing into mongo nodes finished.")
        return genome_nodes, info_nodes, edges


class VCFParser_dbSNP(VCFParser):
    """
    VCFParser_dbSNP is a subclass of VCFParser class
    It is specifically designed to convert the dbSNP data into MongoDB documents.

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
    get_mongo_nodes
    * inherited from parent class *
    parse
    parse_chunk
    parse_one_line_data
    jsondata
    save_json
    load_json
    save_mongo_nodes
    hash

    Examples
    --------
    Initiate a VCFParser_dbSNP:

    >>> parser = VCFParser_dbSNP("clinvar.vcf")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    Get the Mongo nodes

    >>> mongo_nodes = parser.get_mongo_nodes()

    Save the Mongo nodes to a file

    >>> parser.save_mongo_nodes('output.mongonodes')

    """
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
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.varients,
        which are contents of self.data
        2. The GenomeNodes generated from this parsing should be SNPs. They should all have "_id" started with "G". The rs# will be used, like "Gsnp_rs4950928".
        3. The InfoNodes generated only contain 1 dataSource. It has "_id" values start with "I", like "IdbSNP".
        4. No Edges are given.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = VCFParser_ClinVar('GWAS.tsv')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        The GenomeNodes contain the information for the SNPs:

        >>> print(genome_nodes[0])
        {
            "_id": "Gsnp_rs367896724",
            "assembly": "GRCh38",
            "chromid": 1,
            "type": "SNP",
            "start": 10177,
            "end": 10177,
            "length": 1,
            "source": "dbSNP",
            "name": "RS367896724",
            "info": {
                "RS": 367896724,
                "RSPOS": 10177,
                "dbSNPBuildID": 138,
                "SSR": 0,
                "SAO": 0,
                "VP": "0x050000020005170026000200",
                "GENEINFO": "DDX11L1:100287102",
                "WGT": 1,
                "VC": "DIV",
                "R5": true,
                "ASP": true,
                "VLD": true,
                "G5A": true,
                "G5": true,
                "KGPhase3": true,
                "CAF": "0.5747,0.4253",
                "COMMON": 1,
                "variant_ref": "A",
                "variant_alt": "AC",
                "filter": ".",
                "qual": "."
            }
        }

        Only one InfoNode is given with the type dataSource:

        >>> print(info_nodes[0])
        {
            "_id": "IdbSNP",
            "type": "dataSource",
            "name": "dbSNP",
            "source": "dbSNP",
            "info": {
                "filename": "test_dbSNP.vcf",
                "INFO": {
                "RS": {
                    "Type": "Integer",
                    "Description": "dbSNP ID (i.e. rs number)"
                },
                "RSPOS": {
                    "Type": "Integer",
                    "Description": "Chr position reported in dbSNP"
                },
                "RV": {
                    "Type": "Flag",
                    "Description": "RS orientation is reversed"
                },
                "VP": {
                    "Type": "String",
                    "Description": "Variation Property.  Documentation is at ftp://ftp.ncbi.nlm.nih.gov/snp/specs/dbSNP_BitField_latest.pdf"
                },
                ...
            }
        }

        No Edges are generated:

        >>> print(edges)
        []

        """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node= {
            "_id": 'I'+DATA_SOURCE_DBSNP,
            "type": "dataSource",
            'name':DATA_SOURCE_DBSNP,
            "source": DATA_SOURCE_DBSNP,
            'info': self.metadata.copy()
        }
        info_nodes.append(info_node)
        known_vid = set()
        if 'reference' in self.metadata:
            assembly = self.metadata['reference'].split('.',1)[0]
        else:
            assembly = 'GRCh38'
        for variant in self.variants:
            d = variant.copy()
            # we will abandon this entry if the CHROM is not recognized
            if d['CHROM'] not in CHROMO_IDXS: continue
            chromid = CHROMO_IDXS[d['CHROM']]
            # create GenomeNode for Varient
            if 'RS' in d['INFO']:
                rs = str(d["INFO"].pop("RS"))
                variant_id = "Gsnp_rs" + rs
                variant_type = "SNP"
                name = 'RS' + rs
            else:
                print(f"Warning, RS number not found, skipping this data {d}")
                continue
            pos = int(d['POS'])
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {
                    '_id': variant_id,
                    'assembly': assembly,
                    'chromid': chromid,
                    'type': 'SNP',
                    'start': pos,
                    'end': pos,
                    'length': 1,
                    'source': DATA_SOURCE_DBSNP,
                    'name': name,
                    'info': d['INFO']
                }
                gnode['info'].update({
                    "variant_ref": d["REF"],
                    'variant_alt': d['ALT'],
                    'filter': d['FILTER'],
                    'qual': d['QUAL']
                })
                genome_nodes.append(gnode)
        return genome_nodes, info_nodes, edges
