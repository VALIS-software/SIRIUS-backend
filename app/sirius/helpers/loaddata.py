from sirius.mongo import GenomeNodes, InfoNodes, Edges
from sirius.helpers.constants import DATA_SOURCE_GENOME, DATA_SOURCE_GWAS, DATA_SOURCE_GTEX, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP, DATA_SOURCE_ENCODE
from sirius.helpers.constants import TRACK_TYPE_GENOME, TRACK_TYPE_GWAS, TRACK_TYPE_EQTL, TRACK_TYPE_ENCODE, ENSEMBL_GENE_SUBTYPES

#----------------------------------------------
# Load all available types of tracks
#----------------------------------------------
def load_mongo_data_information():
    loaded_dataSources = set(InfoNodes.distinct('source'))
    # the description is hard coded here, could be replaced by querying InfoNodes of type dataSource later
    track_type_list = [
        # { 'track_type': TRACK_TYPE_GENOME,
        #   'title': 'Genomic Elements',
        #   'description': 'Genes, Promoters, Enhancers, Binding Sites and more.',
        #   'depends': {DATA_SOURCE_GENOME}
        # },
        { 'track_type': TRACK_TYPE_GWAS,
          'title': 'Genome Wide Associations',
          'description': 'Variants related to traits or diseases.',
          'depends': {DATA_SOURCE_GWAS}
        },
        # { 'track_type': TRACK_TYPE_EQTL,
        #   'title': 'Quantitative Trait Loci',
        #   'description': 'Variants related to changes in gene expression or other quantitative measures.',
        #   'depends': {DATA_SOURCE_GTEX}
        # },
        { 'track_type': TRACK_TYPE_ENCODE,
          'title': 'Encyclopedia of DNA Elements',
          'description': 'Comprehensive parts list of functional elements in the human genome.',
          'depends': {DATA_SOURCE_ENCODE}
        },
    ]
    track_types_info = []
    for t in track_type_list:
        if any(d in loaded_dataSources for d in t.pop('depends')):
            track_types_info.append(t)
    return track_types_info

loaded_track_types_info = load_mongo_data_information()


#-------------------------------
# Load data track information
#-------------------------------
def load_data_track_information():
    data_track_info_dict = dict()
    for data in InfoNodes.find({'type': {'$in':['sequence', 'signal']}}):
        # we take out the 'I' in front of the InfoNode _id
        tid = data['_id'][1:]
        data_track_info_dict[tid] = {
            'id': tid,
            'name': data['name'],
            'type': data['type'],
            'source': data['source'],
            'contig_info': dict([ (contig['contig'], contig) for contig in data['info']['contigs'] ])
        }
    return data_track_info_dict

loaded_data_track_info_dict = load_data_track_information()
loaded_data_tracks = list(loaded_data_track_info_dict.values())


#------------------------------
# Load contig information
#------------------------------
def load_contig_information():
    loaded_contig_info_dict = dict()
    # here we load the contig info from the data track info
    seq_info = loaded_data_track_info_dict.get('sequence', None)
    contig_info = seq_info.get('contig_info', None) if seq_info else None
    if contig_info:
        for name, data in contig_info.items():
            loaded_contig_info_dict[name] = {
                'name': data['contig'],
                'start': data.get('start', 1),
                'length': data['length'],
            }
    return loaded_contig_info_dict

loaded_contig_info_dict = load_contig_information()
loaded_contig_info = list(loaded_contig_info_dict.values())


# Store all possible genome contigs
# this should be normalized with the InfoNodes in the future
loaded_genome_contigs = set(GenomeNodes.distinct('contig'))

# store all loaded genes
loaded_gene_names = sorted(GenomeNodes.distinct('name', {'type': {'$in': ENSEMBL_GENE_SUBTYPES}}))
print(f'Loaded {len(loaded_gene_names)} genes')

# store all loaded traits (sorted)
loaded_trait_names = sorted(InfoNodes.distinct('name', {'type': 'trait'}))
print(f'Loaded {len(loaded_trait_names)} traits')

# store all loaded cell types
loaded_cell_types = sorted(InfoNodes.distinct('info.biosample', {'type': 'ENCODE_accession'}))
print(f'Loaded {len(loaded_cell_types)} cell types')

# store all loaded tumor_tissue_sites
loaded_patient_tumor_sites = sorted([s for s in InfoNodes.distinct('info.biosample', {'type':'patient'}) if s])
print(f'Loaded {len(loaded_patient_tumor_sites)} patient tumor sites')

# store all loaded info.targets
loaded_info_targets = sorted(InfoNodes.distinct('info.targets'))
print(f'Loaded {len(loaded_info_targets)} info.targets')

# store all loaded info.pathway
loaded_pathway_names = sorted(InfoNodes.distinct('name',  {'type': 'pathway'}))
print(f'Loaded {len(loaded_pathway_names)} pathway')

# type-specific cell types for encode tokenbox
loaded_cell_types_promoter = sorted(InfoNodes.distinct('info.biosample', {'type': 'ENCODE_accession', 'info.types': 'Promoter-like'}))
print(f'Loaded {len(loaded_cell_types_promoter)} cell types for promoters')
loaded_cell_types_enhancer = sorted(InfoNodes.distinct('info.biosample', {'type': 'ENCODE_accession', 'info.types': 'Enhancer-like'}))
print(f'Loaded {len(loaded_cell_types_enhancer)} cell types for enhancers')

# type-specific cell type for eQTL tokenbox
loaded_cell_types_eqtl = sorted(InfoNodes.distinct('info.biosample', {'source': 'GTEx'}))
print(f'Loaded {len(loaded_cell_types_eqtl)} cell types for eQTLs')
