import os, copy
from sirius.parsers.Parser import Parser
from sirius.helpers.constants import CHROMO_IDXS, DATA_SOURCE_TCGA
import xml.etree.ElementTree as ET

class TCGA_XMLParser(Parser):
    """
    Parser for the TCGA BCR XML file format.
    One such xml file will be parsed into a dictionary that contains data for one patient
    """
    @property
    def patientdata(self):
        return self.data['patientdata']

    @patientdata.setter
    def patientdata(self, value):
        self.data['patientdata'] = value

    def parse(self):
        self.filehandle.seek(0)
        self.patientdata = dict()
        tree = ET.parse(self.filehandle)
        root = tree.getroot()
        patient = next(child for child in root if child.tag.endswith('patient'))
        for data in patient:
            key = data.tag.split('}')[-1]
            self.patientdata[key] = data.text

    def get_mongo_nodes(self):
        genome_nodes, info_nodes, edges = [], [], []
        p = self.patientdata
        # create one infonode for this patient
        info_nodes = [{
            '_id': 'Ipatient' + p['bcr_patient_barcode'],
            'type': 'patient',
            'name': 'Patient ' + p['patient_id'],
            'source': DATA_SOURCE_TCGA,
            'info': {
                'patient_id': p['patient_id'],
                'bcr_patient_uuid': p['bcr_patient_uuid'],
                'bcr_patient_barcode': p['bcr_patient_barcode'],
                'days_to_birth': int(p['days_to_birth']),
                'gender': p['gender'],
                'tumor_tissue_site': p['tumor_tissue_site'],
                'ethnicity': p['ethnicity'],
                'diagnosis': p['diagnosis'],
            }
        }]
        return genome_nodes, info_nodes, edges


class TCGA_MAFParser(Parser):
    """
    Parser for the TCGA .maf file format

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
    1. The .maf file contain tab-separated values in lines.
    2. The first few lines starting with # contains metadata.
    3. The first line not starting with # contain the column labels.

    References
    ----------
    https://docs.gdc.cancer.gov/Data/File_Formats/MAF_Format/

    Examples
    --------
    Initiate a MAFParser:

    >>> parser = MAFParser("TCGA.maf")

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
    def mutations(self):
        return self.data['mutations']

    @mutations.setter
    def mutations(self, value):
        self.data['mutations'] = value

    def parse(self):
        """
        Parse the TCGA maf data format.

        Notes
        -----
        1. This method will move the openned self.filehandle to beginning of file, then read from it.
        2. The .maf contain tab-separated values in lines.
        3. The first few lines starting with # contains metadata.
        4. The first line not starting with # contain the column labels.

        References
        ----------
        https://docs.gdc.cancer.gov/Data/File_Formats/MAF_Format/

        Examples
        --------
        Initialize and parse the file:

        >>> parser = MAFParser("TCGA.maf")
        >>> parser.parse()

        The parsed data are stored in self.data, which contains self.metadata and self.mutations:

        >>> print(parser.mutations[0])
        {
        }

        """
        # start from the beginning for reading
        self.filehandle.seek(0)
        self.mutations = []
        # read the first line as labels
        labels = None
        # read the rest of the lines as data
        for line in self.filehandle:
            line = line.strip()
            if line[0] == '#':
                key, value = line[1:].split(maxsplit=1)
                self.metadata[key] = value
            elif line:
                if labels == None:
                    labels = line.split('\t')
                else:
                    ls = line.split('\t')
                    self.mutations.append(dict(zip(labels, ls)))
                    if self.verbose and len(self.mutations) % 100000 == 0:
                        print("%d data parsed" % len(self.mutations), end='\r')

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
        info_node = {"_id": 'I'+DATA_SOURCE_TCGA, "type": "dataSource", "name": DATA_SOURCE_TCGA, "source": DATA_SOURCE_TCGA}
        info_node['info'] = self.metadata.copy()
        info_nodes.append(info_node)
        for mut in copy.deepcopy(self.mutations):
            pass
        return genome_nodes, info_nodes, edges
