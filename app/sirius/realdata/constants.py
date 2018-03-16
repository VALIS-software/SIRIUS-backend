chromo_names = [ str(i) for i in list(range(1,23))+['X','Y'] ]
chromo_idxs = dict([(name,i) for i,name in enumerate(chromo_names, 1)])

QUERY_TYPE_GENOME = 'GenomeNode'
QUERY_TYPE_INFO = 'InfoNode'
QUERY_TYPE_EDGE = 'EdgeNode'