from sirius.parsers.parser import Parser
from sirius.helpers.constants import CHROMO_IDXS, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP, DATA_SOURCE_ExAC
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

        v4.2:
        https://gist.github.com/inutano/f0a2f5c219ab4920c5b5

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
        assert self.parse_chunk(size=float('inf')) == True, '.parse_chunk() did not finish parsing the entire file.'

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
                                self.metadata['INFO'][name] = {
                                    'Number': fmtdict['Number'],
                                    'Type': fmtdict['Type'],
                                    "Description": descpt
                                    }
                                # special parsing for CSQ of ExAC dataset
                                if name == "CSQ":
                                    CSQ_labels = descpt.split('Format: ')[1].split('|')
                                    self.metadata['INFO']['CSQ_LABELS'] = CSQ_labels
                        else:
                            self.metadata[ls[0]] = ls[1]
                else:
                    # title line
                    self.labels = line[1:].split()
                    assert self.labels == ["CHROM", "POS", "ID", "REF", "ALT", "QUAL", "FILTER", "INFO"]
            elif line:
                d = self.parse_one_line_data(line)
                self.variants.append(d)
                if self.verbose and len(self.variants) % 100000 == 0:
                    print("%d data parsed" % len(self.variants), end='\r')
                if len(self.variants) >= size:
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
        variant: dictionary
            The dictionary contains the data for this line

        Notes
        -----
        1. The INFO block will be further splitted by the ";".
        2. After splitting, if one phrase has an "=" sign, like "A=ss", it will be parsed into a {key:value} pair like {"A": "ss"} in the d["INFO"] dictionary.
        3. If one phrase is a flag, like "FL", it will be added into the "info['flags']" list, like info['flags'] = ["FL"]

        """
        ls = line.strip().split('\t')
        assert len(ls) == len(self.labels), "Error parsing this line:\n%s with %s split" % (line, len(ls))
        d = dict(zip(self.labels, ls))
        dinfo = {'flags': []}
        for keyandvalue in d['INFO'].split(';'):
            if '=' in keyandvalue:
                k, v = keyandvalue.split('=', 1)
                info_key_meta = self.metadata['INFO'][k]
                vtype = str_to_type(info_key_meta['Type'])
                vnumber = info_key_meta['Number']
                if vnumber == 'A':
                    dinfo[k] = [vtype(i) for i in v.split(',')]
                elif vnumber == '1':
                    dinfo[k] = vtype(v)
                elif k == "CSQ":
                    dinfo.setdefault("CSQs", [])
                    # special parsing for the CSQ field from ExAC database
                    for CSQstring in v.split(','):
                        CSQdict = dict(zip(self.metadata['INFO']['CSQ_LABELS'], CSQstring.split('|')))
                        dinfo["CSQs"].append(CSQdict)
                else:
                    dinfo[k] = v
            else:
                k = keyandvalue
                vtype = self.metadata['INFO'][k]['Type']
                assert vtype.lower() == 'flag', f'{line}\nWarning! No "=" found in the key {k}, but its Type is not Flag'
                dinfo['flags'].append(k)
        d['INFO'] = dinfo
        return d

    def match_ref_alt_1(self, ref, alt):
        """ Match two strings to form the best REF ALT pair
        * This function is not called any more during parsing, but we keep it here in case we need it later
        """
        #assert ref != alt, f"Error: Ref {ref} and Alt {alt} are the same!"
        # already unique if ref or alt is of length 1
        if len(ref) == 1 or len(alt) == 1:
             return 0, ref, alt
        # remove redundant suffix in same position
        # we keep at least one character in ref or alt, consistent with VCF format
        for i in range(min(len(ref), len(alt)) - 1):
            if ref[-1] == alt[-1]:
                ref = ref[:-1]
                alt = alt[:-1]
            else:
                break
        else:
            return 0, ref, alt
        # remove common prefix and shift starting position
        shift = 0
        # we keep at least one character in ref or alt, consistent with VCF format
        for i in range(min(len(ref), len(alt)) - 1):
            if ref[0] == alt[0]:
                ref = ref[1:]
                alt = alt[1:]
                shift += 1
            else:
                break
        return shift, ref, alt

    def match_ref_alt(self, ref, alt):
        """ Match two strings to form the best REF ALT pair
        This is a less efficient way but works better with the ExAC VCF file.
        * This function is not called any more during parsing, but we keep it here in case we need it later
        """
        #assert ref != alt, f"Error: Ref {ref} and Alt {alt} are the same!"
        # already unique if ref or alt is of length 1
        if len(ref) == 1 or len(alt) == 1:
             return 0, ref, alt
        # remove common prefix and shift starting position
        shift = 0
        # we keep at least one character in ref or alt, consistent with VCF format
        for i in range(min(len(ref), len(alt))):
            if ref[0] == alt[0]:
                last_match = ref[0]
                ref = ref[1:]
                alt = alt[1:]
                shift += 1
            else:
                break
        else:
            # if all match, we finish
            shift -= 1
            ref = last_match + ref
            alt = last_match + alt
            return shift, ref, alt
        # remove redundant suffix in same position
        for i in range(min(len(ref), len(alt))):
            if ref[-1] == alt[-1]:
                ref = ref[:-1]
                alt = alt[:-1]
            else:
                break
        else:
            # if all match, we keep one last match from the left side
            shift -= 1
            ref = last_match + ref
            alt = last_match + alt
        return shift, ref, alt

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
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.variants,
        which are contents of self.data
        2. The GenomeNodes generated from this parsing should be Variants. They should all have "_id" started with "G".
        3. If the variant is a SNP, the rs# will be used, like "Gsnp_rs4950928".
        4. If the variant is not a SNP, the content of the variant will be hashed to make the _id like "Gv_28120123ewe129301"
            Duplicated SNPs with the same _id are ignored.
        5. Each variant can be associated with multiple traits.
        6. The InfoNodes generated contain 1 dataSource and multiple traits. They should all have "_id" values start with "I", like "IGWAS", or "Itrait_79485.."
        7. The description of the trait is read from the "INFO.CLINDN" block, separated by "|", with number of traits matching the number identifiers in CLNDISDB.
        8. The _id for the traits are computed as a hash of the trait description in lower case. The "_" in the description is replaced by " ",
        to improve the text search in MongoDB, and match the descriptions of GWAS data set. Duplicated traits with the same _id are ignored.
        9. The Edges generated are connections between the variants and traits. Each edge should have an "_id" starts with "E", and "from_id" = the variant's _id,
        and "to_id" = the trait's _id. All edges have the type "association" for now. Details of the GWAS studies are put in to the 'info' block of the edges.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = VCFParser_ClinVar('GWAS.tsv')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        The GenomeNodes contain the information for the SNPs or variants:

        >>> print(genome_nodes[0])
        {
            "_id": "Gsnp_rs143888043",
            "contig": "chr1",
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
                "MC": "SO:0001583|missense_variant",
                "ORIGIN": "1",
                "CLNHGVS": "NC_000001.11:g.1014042G>A",
                "variant_affected_genes": [
                    "ISG15"
                ]
            }
        },


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
        for d in self.variants:
            # we will abandon this entry if no trait information found
            if 'CLNDN' not in d['INFO'] or 'CLNDISDB' not in d['INFO']: continue
            # create GenomeNode for Variants
            contig = 'chr' + d['CHROM']
            if 'RS' in d['INFO']:
                rs = str(d["INFO"]["RS"]).split('|',1)[0]
                variant_id = "Gsnp_rs" + rs
                variant_type = "SNP"
                name = 'RS' + rs
            else:
                variant_type = 'variant' #d['INFO']['CLNVC'].lower()
                pos = str(d['POS'])
                v_ref, v_alt = d['REF'], d['ALT']
                variant_key_string = '_'.join([contig, pos, v_ref, v_alt])
                variant_id = 'Gv_' + self.hash(variant_key_string)
                name = ' '.join(s.capitalize() for s in variant_type.split('_'))
            pos = int(d['POS'])
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {
                    '_id': variant_id,
                    'contig':contig,
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
                for key in ('ALLELEID', 'CLNVCSO', 'MC', 'ORIGIN', 'CLNHGVS'):
                    value = d["INFO"].pop(key, None)
                    if value != None:
                        gnode['info'][key] = value
                variant_affected_genes = []
                geneinfo = d['INFO'].pop('GENEINFO', None)
                if geneinfo != None:
                    for ginfo in geneinfo.split('|'):
                        variant_affected_genes.append(ginfo.split(':')[0])
                gnode['info']['variant_affected_genes'] = variant_affected_genes
                genome_nodes.append(gnode)
            # create InfoNode for trait, one entry could have multiple traits
            this_trait_ids = []
            trait_names = d['INFO']['CLNDN'].split('|')
            trait_CLNDISDBs = d['INFO']['CLNDISDB'].split('|')
            if len(trait_names) != len(trait_CLNDISDBs):
                print(f"Number of traits in in CLNDN and CLNDISDB not consistent! Skipping this data {d}")
                continue
            for trait_name, trait_disdb in zip(trait_names, trait_CLNDISDBs):
                trait_desp = trait_name.replace("_"," ")
                trait_id = 'Itrait' + self.hash(trait_desp.lower())
                this_trait_ids.append(trait_id)
                #short_name = (''.join(s[0] for s in trait_desp.split())).upper()
                if trait_id not in known_traits:
                    infonode = {
                        '_id': trait_id,
                        'type': 'trait',
                        'name': trait_desp,
                        'source': DATA_SOURCE_CLINVAR,
                        'info': dict()
                    }
                    for nameidx in trait_disdb.split(','):
                        if ':' in nameidx:
                            name, idx = nameidx.split(':', 1)
                            infonode['info'][name] = idx
                    infonode['info']['description'] = trait_desp
                    info_nodes.append(infonode)
                    known_traits.add(trait_id)
            # create EdgeNode for each trait in this entry
            for trait_id in this_trait_ids:
                # add study to edges
                edge = {
                    'from_id': variant_id,
                    'to_id': trait_id,
                    'type': f'association:{variant_type}:trait',
                    'source': DATA_SOURCE_CLINVAR,
                    'name': 'ClinVar Study',
                    'info': {
                        'CLNREVSTAT': d['INFO']["CLNREVSTAT"],
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
            Each of the three is a list of dictionaries, which contains the parsed data.

        Notes
        -----
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.variants,
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
            "contig": "chr1",
            "type": "SNP",
            "start": 10177,
            "end": 10177,
            "length": 1,
            "source": "dbSNP",
            "name": "RS367896724",
            "info": {
                "flags": [
                    "R5",
                    "ASP",
                    "VLD",
                    "G5A",
                    "G5",
                    "KGPhase3"
                ],
                "RSPOS": 10177,
                "dbSNPBuildID": 138,
                "SSR": 0,
                "SAO": 0,
                "VP": "0x050000020005170026000200",
                "WGT": 1,
                "VC": "DIV",
                "variant_ref": "A",
                "variant_alt": "AC",
                "filter": ".",
                "qual": ".",
                "allele_frequencies": {
                    "AC": 0.4253
                },
                "variant_tags": [
                    "is_common"
                ],
                "variant_affected_genes": [
                    "DDX11L1"
                ]
            }
        },

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
                    ,
                    "variant_tags": [
                        "is_common"
                    ]
                }
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
        all_variant_tags = set()
        for variant in self.variants:
            d = variant.copy()
            contig = 'chr' + d['CHROM']
            # create GenomeNode for Variants
            if 'RS' in d['INFO']:
                rs = str(d["INFO"].pop("RS"))
                variant_id = "Gsnp_rs" + rs
                variant_type = "SNP"
                name = 'RS' + rs
            else:
                print(f"Warning, RS number not found, skipping this data {d}")
                continue
            pos = int(d['POS'])
            variant_affected_genes = []
            geneinfo = d['INFO'].pop('GENEINFO', None)
            if geneinfo != None:
                for ginfo in geneinfo.split('|'):
                    variant_affected_genes.append(ginfo.split(':')[0])
            # try to read allele frequencies from CAF field (1000Genomes)
            afstring = d['INFO'].pop('CAF', None)
            # try to use the TOPMED number if CAF not available
            if afstring == None:
                afstring = d['INFO'].pop('TOPMED', None)
            allele_frequencies = {}
            if afstring != None:
                afs = afstring.split(',')[1:]
                alleles = d['ALT'].split(',')
                assert len(afs) == len(alleles)
                for alt, af in zip(alleles, afs):
                    if af == '.':
                        allele_frequencies[alt] = 0
                    else:
                        allele_frequencies[alt] = float(af)
            variant_tags = []
            if d['INFO'].pop("COMMON", None) == 1:
                variant_tags.append('is_common')
                all_variant_tags.add('is_common')
            if variant_id not in known_vid:
                known_vid.add(variant_id)
                gnode = {
                    '_id': variant_id,
                    'contig': contig,
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
                    'qual': d['QUAL'],
                    'allele_frequencies': allele_frequencies,
                    'variant_tags': variant_tags,
                    'variant_affected_genes': variant_affected_genes
                })
                genome_nodes.append(gnode)
        info_nodes[0]['info']['variant_tags'] = list(all_variant_tags)
        return genome_nodes, info_nodes, edges

class VCFParser_ExAC(VCFParser):
    def get_mongo_nodes(self):
        """
        Parse self.data into three types for Mongo nodes, which are the internal data structure in our MongoDB.

        Returns
        -------
        mongonodes: tuple
            The return tuple is (genome_nodes, info_nodes, edges)
            Each of the three is a list of dictionaries, which contains the parsed data.

        Notes
        -----
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.variants,
        which are contents of self.data
        2. The GenomeNodes generated from this parsing should be variants. They should all have "_id" started with "G". The rs# will be used, like "Gsnp_rs4950928".
        3. Since ExAC VCF file contains a lot of redundant data, we only pick the ones we're interested.
        4. The InfoNodes generated only contain 1 dataSource. It has "_id" values start with "I", like "IdbSNP".
        5. No Edges are given.
        6. The CSQ sub-documents have these labels:
        CSQ_LABELS = "Allele|Consequence|IMPACT|SYMBOL|Gene|Feature_type|Feature|BIOTYPE|EXON|INTRON|\
        HGVSc|HGVSp|cDNA_position|CDS_position|Protein_position|Amino_acids|Codons|Existing_variation|\
        ALLELE_NUM|DISTANCE|STRAND|FLAGS|VARIANT_CLASS|MINIMISED|SYMBOL_SOURCE|HGNC_ID|CANONICAL|TSL|APPRIS|\
        CCDS|ENSP|SWISSPROT|TREMBL|UNIPARC|GENE_PHENO|SIFT|PolyPhen|DOMAINS|HGVS_OFFSET|GMAF|AFR_MAF|AMR_MAF|\
        EAS_MAF|EUR_MAF|SAS_MAF|AA_MAF|EA_MAF|ExAC_MAF|ExAC_Adj_MAF|ExAC_AFR_MAF|ExAC_AMR_MAF|ExAC_EAS_MAF|\
        ExAC_FIN_MAF|ExAC_NFE_MAF|ExAC_OTH_MAF|ExAC_SAS_MAF|CLIN_SIG|SOMATIC|PHENO|PUBMED|MOTIF_NAME|MOTIF_POS|\
        HIGH_INF_POS|MOTIF_SCORE_CHANGE|LoF|LoF_filter|LoF_flags|LoF_info|context|ancestral"

        Examples
        --------
        Initialize and parse the file:

        >>> parser = VCFParser_ExAC('GWAS.tsv')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        The GenomeNodes contain the information for the SNPs or variants:

        >>> print(genome_nodes[0])
        {
            "_id": "Gsnp_rs752859895",
            "contig": "chr1",
            "type": "SNP",
            "start": 13372,
            "end": 13372,
            "length": 1,
            "source": "ExAC",
            "name": "RS752859895",
            "info": {
                "variant_ref": "G",
                "variant_alt": "C",
                "filter": "PASS",
                "qual": "608.91",
                "allele_frequencies": {
                    "C": 6.998e-05
                },
                "variant_tags": [
                    "regulatory_region_variant",
                    "downstream_gene_variant",
                    "non_coding_transcript_exon_variant",
                    "intron_variant",
                    "non_coding_transcript_variant",
                    "splice_region_variant"
                ],
                "variant_affected_genes": [
                    "DDX11L1",
                    "WASH7P"
                ],
                "variant_affected_feature_types": [
                    "RegulatoryFeature",
                    "Transcript"
                ],
                "variant_affected_bio_types": [
                    "transcribed_unprocessed_pseudogene",
                    "processed_transcript",
                    "CTCF_binding_site",
                    "unprocessed_pseudogene"
                ]
            }
        },

        Only one InfoNode is given with the type dataSource:

        >>> print(info_nodes[0])
        {
            "_id": "IExAC",
            "type": "dataSource",
            "name": "ExAC",
            "source": "ExAC",
            "info": {
                "filename": "test.vcf",
                "INFO": {
                "AC": {
                    "Number": "A",
                    "Type": "Integer",
                    "Description": "Allele count in genotypes, for each ALT allele, in the same order as listed"
                },
                "AC_AFR": {
                    "Number": "A",
                    "Type": "Integer",
                    "Description": "African/African American Allele Counts"
                },
                "AC_AMR": {
                    "Number": "A",
                    "Type": "Integer",
                    "Description": "American Allele Counts"
                },
                "AC_Adj": {
                    "Number": "A",
                    "Type": "Integer",
                    "Description": "Adjusted Allele Counts"
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
            "_id": 'I'+DATA_SOURCE_ExAC,
            "type": "dataSource",
            'name':DATA_SOURCE_ExAC,
            "source": DATA_SOURCE_ExAC,
            'info': self.metadata.copy()
        }
        info_nodes.append(info_node)
        # create genome_nodes for each variant
        all_variant_tags = set()
        for d in self.variants:
            alleles = d['ALT'].split(',')
            allele_frequencies = dict(zip(alleles, d['INFO']['AF']))
            variant_tags = set()
            variant_affected_genes = set()
            #impacts = set()
            variant_affected_feature_types = set()
            variant_affected_bio_types = set()
            rs_number = None
            for csq in d['INFO']['CSQs']:
                variant_tags.update(csq['Consequence'].split('&'))
                #impacts.update(csq['IMPACT'])
                variant_affected_genes.add(csq['SYMBOL'])
                variant_affected_feature_types.add(csq['Feature_type'])
                variant_affected_bio_types.add(csq['BIOTYPE'])
                if csq['LoF'] == 'HC':
                    variant_tags.add("loss_of_function")
                rs_number = csq['Existing_variation'] or rs_number
            # remove empty string ''
            for s in (variant_tags, variant_affected_genes, variant_affected_feature_types, variant_affected_bio_types):
                s.discard("")
            contig = 'chr' + d['CHROM']
            pos = int(d['POS'])
            if rs_number != None and rs_number[:2] == 'rs':
                # this solve bug for rs184649466&COSM3486335&COSM3486336
                rs_number = rs_number.split('&',1)[0]
                gid = "Gsnp_" + rs_number
                gtype = 'SNP'
                name = rs_number.upper()
            else:
                variant_key_string = '_'.join([contig, d['POS'], d['REF'], d['ALT']])
                gid = 'Gv_' + self.hash(variant_key_string)
                gtype = 'variant'
                name = 'Variant'
            gnode = {
                '_id': gid,
                'contig': contig,
                'type': gtype,
                'start': pos,
                'end': pos,
                'length': 1,
                'source': DATA_SOURCE_ExAC,
                'name': name,
                'info': {
                    'variant_ref': d["REF"],
                    'variant_alt': d['ALT'],
                    'filter': d['FILTER'],
                    'qual': d['QUAL'],
                    'allele_frequencies': allele_frequencies,
                    'variant_tags': list(variant_tags),
                    'variant_affected_genes': list(variant_affected_genes),
                    #'impacts': list(impacts),
                    'variant_affected_feature_types': list(variant_affected_feature_types),
                    'variant_affected_bio_types': list(variant_affected_bio_types),
                }
            }
            genome_nodes.append(gnode)
            all_variant_tags.update(variant_tags)
        info_nodes[0]['info']['variant_tags'] = list(all_variant_tags)
        return genome_nodes, info_nodes, edges
