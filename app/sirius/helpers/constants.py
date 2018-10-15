CHROMO_NAMES = [ str(i) for i in list(range(1,23)) ] + ['X','Y','MT']
CHROMO_IDXS = dict([(name,i) for i,name in enumerate(CHROMO_NAMES, 1)])
KNOWN_CONTIGS = set('chr'+s for s in CHROMO_NAMES)

QUERY_TYPE_GENOME = 'GenomeNode'
QUERY_TYPE_INFO = 'InfoNode'
QUERY_TYPE_EDGE = 'EdgeNode'

DATA_SOURCE_GENOME = 'GRCh38_gff'
DATA_SOURCE_GWAS = 'GWAS Catalog'
DATA_SOURCE_EXSNP = 'exSNP'
DATA_SOURCE_CLINVAR = 'ClinVar'
DATA_SOURCE_DBSNP = 'dbSNP'
DATA_SOURCE_ENCODE = 'ENCODE'
DATA_SOURCE_FASTA = 'RefSeq'
DATA_SOURCE_EFO = 'EFO'
DATA_SOURCE_ENCODEbigwig = "ENCODEbigwig"
DATA_SOURCE_ExAC = "ExAC"
DATA_SOURCE_TCGA = 'TCGA'
DATA_SOURCE_ENSEMBL = 'ENSEMBL'
DATA_SOURCE_GTEX = 'GTEx'
DATA_SOURCE_HGNC = 'HGNC'
DATA_SOURCE_23ANDME = '23andMe'
DATA_SOURCE_KEGG = 'KEGG'
DATA_SOURCE_NATURE_CAUSAL_VARIANTS = "Nature-Causal-Variants"

TRACK_TYPE_SEQUENCE = 'track_type_sequence'
TRACK_TYPE_FUNCTIONAL = 'track_type_functional'
TRACK_TYPE_GENOME = 'track_type_genome'
TRACK_TYPE_GWAS = 'track_type_gwas'
TRACK_TYPE_EQTL = 'track_type_eqtl'
TRACK_TYPE_ENCODE = 'track_type_encode'
TRACK_TYPE_3D = 'track_type_3d'
TRACK_TYPE_NETWORK = 'track_type_network'
TRACK_TYPE_BOOLEAN = 'track_type_boolean'

ENCODE_COLOR_TYPES = {
    (255,0,0): 'Promoter-like',
    (255,205,0): 'Enhancer-like',
    (0,176,240): 'CTCF-only',
    (6,218,147): 'DNase-only',
    (225,225,225): 'Inactive',
    (140,140,140): 'Unclassified'
}

# The samplingrate threshold for annotation track to return aggregations
AGGREGATION_THRESH = 5000

TILE_DB_BIGWIG_DOWNSAMPLE_RESOLUTIONS = [32, 128, 256, 1024, 16384, 65536, 131072]

SYNONYMS = {
    'hg19': 'GRCh37'
}

SEQ_CONTIG = {
    'NC_000001.11': 'chr1',
    'NC_000002.12': 'chr2',
    'NC_000003.12': 'chr3',
    'NC_000004.12': 'chr4',
    'NC_000005.10': 'chr5',
    'NC_000006.12': 'chr6',
    'NC_000007.14': 'chr7',
    'NC_000008.11': 'chr8',
    'NC_000009.12': 'chr9',
    'NC_000010.11': 'chr10',
    'NC_000011.10': 'chr11',
    'NC_000012.12': 'chr12',
    'NC_000013.11': 'chr13',
    'NC_000014.9': 'chr14',
    'NC_000015.10': 'chr15',
    'NC_000016.10': 'chr16',
    'NC_000017.11': 'chr17',
    'NC_000018.10': 'chr18',
    'NC_000019.10': 'chr19',
    'NC_000020.11': 'chr20',
    'NC_000021.9': 'chr21',
    'NC_000022.11': 'chr22',
    'NC_000023.11': 'chrX',
    'NC_000024.10': 'chrY',
    'MT': 'chrMT',
}

for name in CHROMO_NAMES:
    SEQ_CONTIG[name] = 'chr' + name

ENSEMBL_GENE_SUBTYPES = ['gene', 'pseudogene', 'ncRNA_gene']
