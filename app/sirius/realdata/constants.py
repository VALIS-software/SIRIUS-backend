chromo_names = [ str(i) for i in list(range(1,23))+['X','Y'] ]
chromo_idxs = dict([(name,i) for i,name in enumerate(chromo_names, 1)])

QUERY_TYPE_GENOME = 'GenomeNode'
QUERY_TYPE_INFO = 'InfoNode'
QUERY_TYPE_EDGE = 'EdgeNode'

DATA_SOURCE_GENOME = "GRCh38_gff"
DATA_SOURCE_GWAS = "GWAS"
DATA_SOURCE_EQTL = "eQTL"
DATA_SOURCE_CLINVAR = "ClinVar"
DATA_SOURCE_DBSNP = "dbSNP"
