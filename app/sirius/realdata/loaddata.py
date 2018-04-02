import os
import json
from sirius.core.Annotation import Annotation
from sirius.realdata.constants import chromo_idxs
from sirius.mongo import GenomeNodes, InfoNodes, Edges

def load_mongo_annotations():
    anno_names = GenomeNodes.distinct('assembly')
    loaded_annotations = dict()
    for aname in anno_names:
        chromo_info = dict()
        for d in GenomeNodes.find({'type': 'region', 'assembly': aname, 'info.genome': 'chromosome'}):
            chromoid = d['chromid']
            chromo_info[chromoid] = {'start': d['start'], 'end': d['end']}
        if not chromo_info:
            print("Annotation %s info not found in %s" % (aname, GenomeNodes.name))
            return
        chromo_lengths = [chromo_info[i]['end'] - chromo_info[i]['start'] + 1 for i in chromo_idxs.values()]
        start_bp = chromo_info[1]['start']
        end_bp = sum(chromo_lengths) + start_bp - 1
        adata = {'start_bp': start_bp, 'end_bp': end_bp, 'chromo_lengths': chromo_lengths}
        loaded_annotations[aname] = Annotation(name=aname, datadict=adata)
    return loaded_annotations


loaded_annotations = load_mongo_annotations()

def load_mongo_data_information():
    from sirius.realdata.constants import DATA_SOURCE_GENOME, DATA_SOURCE_GWAS, DATA_SOURCE_EQTL, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP
    from sirius.realdata.constants import TRACK_TYPE_GENOME, TRACK_TYPE_GWAS, TRACK_TYPE_EQTL
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
        }
    ]
    track_info = []
    for t in track_type_list:
        if any(d in loaded_dataSources for d in t.pop('depends')):
            track_info.append(t)
    return track_info

loaded_track_info = load_mongo_data_information()

#def load_type_collection_map():
#    result = dict()
#    for collection in (GenomeNodes, InfoNodes, Edges):
#        distinct_types = collection.distinct('type')
#        for t in distinct_types:
#            if t in result:
#                print("Warning: type %s exist in both %s and %s" % (result[t].name, collection.name))
#            result[t] = collection
#    return result

#type_collection_map = load_type_collection_map()

