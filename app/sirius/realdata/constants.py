chromo_names = [ str(i) for i in list(range(1,23)) ] + ['X','Y']
chromo_idxs = dict([(name,i) for i,name in enumerate(chromo_names, 1)])

QUERY_TYPE_GENOME = 'GenomeNode'
QUERY_TYPE_INFO = 'InfoNode'
QUERY_TYPE_EDGE = 'EdgeNode'

DATA_SOURCE_GENOME = 'GRCh38_gff'
DATA_SOURCE_GWAS = 'GWAS'
DATA_SOURCE_EQTL = 'eQTL'
DATA_SOURCE_CLINVAR = 'ClinVar'
DATA_SOURCE_DBSNP = 'dbSNP'
DATA_SOURCE_ENCODE = 'ENCODE'

TRACK_TYPE_SEQUENCE = 'track_type_sequence'
TRACK_TYPE_FUNCTIONAL = 'track_type_funcional'
TRACK_TYPE_GENOME = 'track_type_genome'
TRACK_TYPE_GWAS = 'track_type_gwas'
TRACK_TYPE_EQTL = 'track_type_eqtl'
TRACK_TYPE_ENCODE = 'track_type_encode'
TRACK_TYPE_3D = 'track_type_3d'
TRACK_TYPE_NETWORK = 'track_type_network'

ENCODE_COLOR_TYPES = {
    (255,0,0): 'Promoter-like',
    (255,205,0): 'Enhancer-like',
    (0,176,240): 'CTCF-only',
    (6,218,147): 'DNase-only',
    (225,225,225): 'Inactive',
    (140,140,140): 'Unclassified'
}
