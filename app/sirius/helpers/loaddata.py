from sirius.mongo import GenomeNodes, InfoNodes, Edges
from sirius.helpers.constants import DATA_SOURCE_GENOME, DATA_SOURCE_GWAS, DATA_SOURCE_EQTL, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP, DATA_SOURCE_ENCODE
from sirius.helpers.constants import TRACK_TYPE_GENOME, TRACK_TYPE_GWAS, TRACK_TYPE_EQTL, TRACK_TYPE_ENCODE

#----------------------------------------------
# Load all available types of tracks
#----------------------------------------------
def load_mongo_data_information():
    loaded_dataSources = set(InfoNodes.distinct('source'))
    # the description is hard coded here, could be replaced by querying InfoNodes of type dataSource later
    track_type_list = [
        { 'track_type': TRACK_TYPE_GENOME,
          'title': 'Genomic Elements',
          'description': 'Genes, Promoters, Enhancers, Binding Sites and more.',
          'depends': {DATA_SOURCE_GENOME}
        },
        { 'track_type': TRACK_TYPE_GWAS,
          'title': 'Genome Wide Associations',
          'description': 'Variants related to traits or diseases.',
          'depends': {DATA_SOURCE_GWAS, DATA_SOURCE_EQTL}
        },
        { 'track_type': TRACK_TYPE_EQTL,
          'title': 'Quantitative Trait Loci',
          'description': 'Variants related to changes in gene expression or other quantitative measures.',
          'depends': {DATA_SOURCE_EQTL}
        },
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


#------------------------------
# Load contig information
#------------------------------
def load_contig_information():
    loaded_contig_info = []
    for data in InfoNodes.find({'type': 'contig'}):
        contig_info = {
            'name': data['name'],
            'length': data['info']['length'],
            'chromosome': data['info'].get('chromosome', 'Unknown')
        }
        loaded_contig_info.append(contig_info)
    return loaded_contig_info

loaded_contig_info = load_contig_information()
loaded_contig_info_dict = dict([(d['name'], d) for d in loaded_contig_info])

#-------------------------------
# Load data track information
#-------------------------------
def load_data_track_information():
    loaded_data_tracks = []
    data_track_info_dict = dict()
    for data in InfoNodes.find({'type': {'$in':['sequence', 'signal']}}):
        data_track_info = {
            # we take out the 'I' in front of the InfoNode _id
            'id': data['_id'][1:],
            'name': data['name'],
            'type': data['type'],
            'source': data['source'],
            'contig_info': dict([ (contig['contig'], contig) for contig in data['info']['contigs'] ])
        }
        loaded_data_tracks.append(data_track_info)
    return loaded_data_tracks

loaded_data_tracks = load_data_track_information()
loaded_data_track_info_dict = dict([(d['id'], d) for d in loaded_data_tracks])

# Store all possible genome contigs
# this should be normalized with the InfoNodes in the future
loaded_genome_contigs = set(GenomeNodes.distinct('contig'))

# store all loaded genes
loaded_gene_names = GenomeNodes.distinct('name', {'type': 'gene'})

# store all loaded traits
loaded_trait_names = InfoNodes.distinct('name', {'type': 'trait'})
