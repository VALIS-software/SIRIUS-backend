import os
import json
import hashlib
import gzip

class Parser(object):
    """
    The parent Parser class. Several methods in common are defined.

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
    jsondata
    save_json
    load_json
    get_mongo_nodes
    save_mongo_nodes
    hash

    Notes
    -----
    1. The parent Parser class defines several useful shared methods for all kind of parsers.
    2. The filehandle is openned in the constructor and closed in the destructor.
    3. For a specific parser as a subclass, the parse() and get_mongo_nodes() should be defined.

    Examples
    --------
    Initialize a Parser:

    >>> from sirius.parsers.Parser import Parser

    This is usually called when defining the subclass of a specific parser:

    >>> class XXXParser(Parser):
    >>>     def parse(self):
    >>>         ...
    >>>     def get_mongo_nodes(self):
    >>>         ...

    """
    @property
    def metadata(self):
        return self.data['metadata']

    @metadata.setter
    def metadata(self, value):
        self.data['metadata'] = value

    def __init__(self, filename, verbose=False):
        """ Initializer of Parser class """
        self.filename = os.path.basename(filename)
        self.verbose = verbose
        self.ext = os.path.splitext(self.filename)[1]
        self.data = {'metadata': {'filename': self.filename}}
        # open the filehandle here for easier control of the reading process
        self.filehandle = gzip.open(filename, 'rt') if self.ext == '.gz' else open(filename)

    def __del__(self):
        """ Destructor """
        self.filehandle.close()

    def parse(self):
        """ Parsing the file, should be implemented in subclasses """
        raise NotImplementedError

    def jsondata(self):
        """Return a json string for self.data"""
        return json.dumps(self.data)

    def save_json(self, filename=None):
        """
        Save a json file with the content of self.data

        Parameters
        ----------
        filename: string, optional
            The output filename which is saved to.
            Default is `self.filename + ".json"`

        """
        if filename == None:
            filename = self.filename + '.json'
        with open(filename, 'w') as out:
            json.dump(self.data, out, indent=2)

    def load_json(self, filename):
        """
        Load a json file into self.data

        Parameters
        ----------
        filename: string
            The input filename which is loaded from

        """
        self.data = json.load(filename)

    def get_mongo_nodes(self):
        """ Getting MongoDB documents after parsing, should be implemented in subclasses """
        raise NotImplementedError

    def save_mongo_nodes(self, filename=None):
        """
        Save the parsed three type of Mongo nodes to a file in json format.

        Parameters
        ----------
        filename: string, optional
            The output filename which is saved to.
            Default is `self.filename + ".mongonodes"`

        Notes
        -----
        The output json file represents a dictionary of three lists
        {
            'genome_nodes': genome_nodes,
            'info_nodes': info_nodes,
            'edges': edges
        }
        The actual data are generated by self.geo_mongo_nodes(), and some of them might be empty.

        """
        if filename == None: filename = self.filename + '.mongonodes'
        genome_nodes, info_nodes, edges = self.get_mongo_nodes()
        d = {'genome_nodes': genome_nodes, 'info_nodes': info_nodes, 'edges': edges}
        json.dump(d, open(filename, 'w'), indent=2)

    def hash(self, string):
        """
        Compute a consistent hash string for a string.

        Parameters
        ----------
        string: string
            The input string to be hashed.

        Returns
        -------
        hash: string
            The hash string.

        """
        return hashlib.sha256(string.encode('utf-8')).hexdigest()
