import re
from sirius.parsers.Parser import Parser
from sirius.helpers.constants import DATA_SOURCE_EFO

class OBOParser(Parser):
    """
    Parser for the OBO data formats.
    based on version 1.2

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
    parse_property_values
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
    version 1.2: http://owlcollab.github.io/oboformat/doc/GO.format.obo-1_2.html

    Examples
    --------
    Initiate a OBOParser:

    >>> parser = OBOParser("efo.obo")

    Parse the file:

    >>> parser.parse()

    Save the parsed data to a json file

    >>> parser.save_json('data.json')

    """

    @property
    def factors(self):
        return self.data['factors']

    @factors.setter
    def factors(self, value):
        self.data['factors'] = value

    def parse(self):
        """
        Parse the OBO data format.

        The file in OBO format is text file in blocks like [Term]

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The lines at the beginning of file before any [Term] is parsed as metadata
        3. Each [Term] block is going to be parsed as an entry in self.factors dictionary.

        References
        ----------
        http://owlcollab.github.io/oboformat/doc/GO.format.obo-1_2.html

        Examples
        --------
        Initialize and parse the file:

        >>> parser = OBOParser("efo.obo")
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.variants:

        >>> print(parser.factors[0])
        {
            "id": "EFO:0000001",
            "name": "experimental factor",
            "def": "An experimental factor in Array Express which are essentially the variable aspects of an experiment design which can be used to describe an experiment, or set of experiments, in an increasingly detailed manner. This upper level class is really used to give a root class from which applications can rely on and not be tied to upper ontology classses which do change.",
            "created_by": "James Malone"
        }

        """
        # restart the reading of the file from the beginning
        self.filehandle.seek(0)
        self.factors = []
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
        5. The metadata should be at the top of the file.

        Examples
        --------
        Initialize parser:

        >>> parser = OBOParser("efo.obo")

        Parse the file and stop at 10 variants.

        >>> parser.parse_chunk(size=10)

        The self.variants list should now contain 10 data.

        >>> print(len(parser.factors)
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
        # reset factors
        self.factors = []
        reading_metadata = True
        reading_term = False
        pvstrings = []
        block_data = dict()
        block_title = 'header'
        for line in self.filehandle:
            # remove '\n' and ignore things after '!'
            line = line.split('!', maxsplit=1)[0].strip()
            # skip empty lines
            if not line: continue
            # title line
            if line[0] == '[' and line[-1] == ']':
                # add data of current block to self.factors
                p_dict = self.parse_property_values(pvstrings)
                block_data.update(p_dict)
                if reading_metadata == True:
                    self.metadata.update(block_data)
                    reading_metadata = False
                else:
                    self.factors.append(block_data.copy())
                    # stop here if reached the chunk size
                    if len(self.factors) == size:
                        if self.verbose:
                            print(f"Parsing file {self.filename} finished for chunk of size {size}" )
                        break
                # reset the block
                pvstrings = []
                block_title = line[1:-1]
                block_data = {'type': block_title}
            elif reading_metadata == True:
                key, value = line.split(': ', maxsplit=1)
                if key in ('subsetdef', 'synonymtypedef'):
                    block_data.setdefault(key, dict())
                    defkey, defvalue = value.split(maxsplit=1)
                    block_data[key][defkey] = defvalue[1:-1] # remove the ""
                elif key == 'property_value':
                    pvstrings.append(value)
            else:
                key, value = line.split(': ', maxsplit=1)
                if key == 'property_value':
                    # The property values needs to be combined and parsed later
                    pvstrings.append(value)
                elif key == 'synonym':
                    block_data.setdefault('synonyms', [])
                    assert value[0] == '"'
                    end = value.rfind('"')
                    block_data['synonyms'].append(value[1:end])
                elif key == 'is_a':
                    block_data.setdefault('is_a', [])
                    block_data['is_a'].append(value)
                elif key == 'def':
                    assert value[0] == '"', "def value should start with double quote \""
                    end = value.rfind('"')
                    value = value[1:end]
                    # remove the redundant "\\n" in def
                    value.replace('\\n', '')
                    block_data['def'] = value
                else:
                    block_data[key] = value
        # if parsing the entire file is finished
        else:
            # add data of current block to self.factors
            p_dict = self.parse_property_values(pvstrings)
            block_data.update(p_dict)
            if block_data:
                self.factors.append(block_data.copy())
            if self.verbose:
                print(f"Parsing the entire file {self.filename} finished.")
            return True
        return False

    def parse_property_values(self, pvstrings):
        """
        Parse the property values in OBO format into a dictionary.

        Parameters
        ----------
        pvstrings: list
            The property values list, each element is a property value string

        Returns
        -------
        p_dict: dictionary
            The properties dictionary

        Notes
        -----
        1. The property value strings should be striped.
        2. The input property values list may contain multiple property values with the same key. The values of those will be combined into a list.
        3. If the type is not specified, will use string as default
        4. The value of each pv should be inside the double quote if it's a string with more than one words

        Examples
        --------
        >>> pvs = [
        >>>     'IAO:0000412 http://purl.obolibrary.org/obo/bto.owl',
        >>>     'http://www.ebi.ac.uk/efo/KEGG_COMPOUND_definition_citation "KEGG COMPOUND:70458-96-7 \"CAS Registry Number\"" xsd:string',
        >>>     'http://www.ebi.ac.uk/efo/Patent_definition_citation "Patent:BE863429 \"Patent\"" xsd:string',
        >>>     'http://www.ebi.ac.uk/efo/Patent_definition_citation "Patent:DE2840910 \"Patent\"" xsd:string'
        >>> ]
        >>> p_dict = parse_property_values(pvs)
        >>> print(p_dict)
        {
            'IAO:0000412': 'http://purl.obolibrary.org/obo/bto.owl',
            'http://www.ebi.ac.uk/efo/KEGG_COMPOUND_definition_citation': 'KEGG COMPOUND:70458-96-7 "CAS Registry Number"',
            'http://www.ebi.ac.uk/efo/Patent_definition_citation': ['Patent:BE863429 "Patent"', 'Patent:DE2840910 \"Patent\"']
        }
        """
        xsdtype = {
            'xsd:simpleType': str,
            'xsd:string': str,
            'xsd:integer': int,
            'xsd:decimal': float,
            'xsd:negativeInteger': int,
            'xsd:positiveInteger': int,
            'xsd:nonNegativeInteger': int,
            'xsd:nonPositiveInteger': int,
            'xsd:boolean': bool,
            'xsd:date': str,
        }
        result = dict()
        if not pvstrings: return result
        for pv_str in pvstrings:
            key, value = pv_str.split(maxsplit=1)
            if value[0] == '"':
                if value[-1] == '"':
                    v = value[1:-1]
                else:
                    end = value.rfind('"')
                    typ = xsdtype.get(value[end+2:], str)
                    v = typ(value[1:end])
            else:
                value_split = value.split()
                if len(value_split) == 1:
                    v = value_split[0]
                elif len(value_split) == 2:
                    typ = xsdtype.get(value_split[1], str)
                    v = typ(value_split[0])
                else:
                    raise RuntimeError(f"Error parsing {pvstrings}, should have max three split, but found {len(value_split)+1}")
            if key in result:
                rvalue = result[key]
                if isinstance(rvalue, list):
                    rvalue.append(v)
                else:
                    result[key] = [rvalue, v]
        return result


class OBOParser_EFO(OBOParser):
    """
    OBOParser_EFO is a subclass of OBOParser class, specifically designed to
    convert the efo.obo data into MongoDB documents.

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
    parse_property_values
    jsondata
    save_json
    load_json
    save_mongo_nodes
    hash

    Examples
    --------
    Initiate a OBOParser_EFO:

    >>> parser = OBOParser_EFO("efo.obo")

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
        1. This method should be called after self.parse(), because this method will read from self.metadata and self.factors,
        which are contents of self.data
        2. No GenomeNodes will be generated from this parsing.
        3. The InfoNodes generated contain 1 dataSource and multiple traits. They should all have "_id" values start with "I", like "IEFO", or "Itrait_ewi832410r.."
        4. Each factor in self.factors will be parsed into one trait. Only the [Term] blocks are used for now.
        5. The _id for the traits are computed as a hash of the trait name in lower case.
        6. Each InfoNode will have the ['info']['EFO_ID'] key to match the original ID in EFO.
        6. No Edges are generated for now.

        Examples
        --------
        Initialize and parse the file:

        >>> parser = OBOParser_EFO('efo.obo')
        >>> parser.parse()

        Get the Mongo nodes:

        >>> genome_nodes, info_nodes, edges = parser.get_mongo_nodes()

        The GenomeNodes contain the information for the SNPs:

        >>> print(genome_nodes)
        []

        The first InfoNode is the dataSource:

        >>> print(info_nodes[0])
        {
            "_id": "IEFO",
            "type": "dataSource",
            "name": "EFO",
            "source": "EFO",
            "info": {
                "filename": "efo.obo",
                "subsetdef": {
                    "abnormal_slim": "abnormal slim",
                    "attribute_slim": "attribute slim",
                    ...
                },
                "synonymtypedef": {
                    "acronym": "acronym",
                    "anamorph": "anamorph",
                    "blast_name": "blast name"
                }
        }

        The rest of the InfoNodes are traits:

        >>> print(info_nodes[1])
        {
            "_id": "Itrait691f5e75225f4dafd632ccfae3fb4f21b061f46ad9243580d7f388b883257823",
            "type": "trait",
            "name": "experimental factor",
            "source": "EFO",
            "info": {
                "type": "Term",
                "created_by": "James Malone",
                "EFO": "0000001",
                "description": "An experimental factor in Array Express which are essentially the variable aspects ..."
            }
        },

        The Edges should be empty:

        >>> print(edges)
        []

        """
        genome_nodes, info_nodes, edges = [], [], []
        # add dataSource into InfoNodes
        info_node= {
            "_id": 'I'+DATA_SOURCE_EFO,
            "type": "dataSource",
            'name':DATA_SOURCE_EFO,
            "source": DATA_SOURCE_EFO,
            'info': self.metadata.copy()
            }
        info_nodes.append(info_node)
        for d in self.factors:
            d = d.copy()
            if d['type'] != 'Term': continue
            name = d.pop('name')
            data_id = d.pop('id')
            # skip every thing other than EFO: and GO: for now
            key, value = data_id.split(':')
            if key not in ('EFO', 'GO'): continue
            d[key] = value
            description = d.pop('def', None)
            trait_id = 'Itrait' + self.hash(name.lower())
            info_node = {
                '_id': trait_id,
                'type': 'trait',
                'name': name,
                'source': DATA_SOURCE_EFO,
                'info': d
            }
            if description != None:
                # fix the redundant \\n in the def
                if description[:2] == '\\n':
                    description = description[2:]
                if description[-2:] == '\\n':
                    description = description[:-2]
                info_node['info']['description'] = description
            info_nodes.append(info_node)
            if self.verbose and len(info_node) % 100000 == 0:
                print("%d traits parsed" % len(info_node), end='\r')
        if self.verbose:
            print("Parsing into mongo nodes finished.")
        return genome_nodes, info_nodes, edges
