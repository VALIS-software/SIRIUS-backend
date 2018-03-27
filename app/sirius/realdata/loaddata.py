import os
import json
from sirius.core.Annotation import Annotation
from sirius.realdata.constants import chromo_idxs
from sirius.mongo import GenomeNodes, InfoNodes, EdgeNodes

def load_mongo_annotations():
    anno_names = GenomeNodes.distinct('assembly')
    loaded_annotations = dict()
    for aname in anno_names:
        chromo_info = dict()
        for d in GenomeNodes.find({'type': 'region', 'assembly': aname, 'info.attributes.genome': 'chromosome'}):
            chromoid = d['chromid']
            chromo_info[chromoid] = {'start': d['start'], 'end': d['end']}
        if not chromo_info:
            print("AnnotationID %s not found in %s" % (annotationId, GenomeNodes.name))
            return
        chromo_lengths = [chromo_info[i]['end'] - chromo_info[i]['start'] + 1 for i in chromo_idxs.values()]
        start_bp = chromo_info[1]['start']
        end_bp = sum(chromo_lengths) + start_bp - 1
        adata = {'start_bp': start_bp, 'end_bp': end_bp, 'chromo_lengths': chromo_lengths}
        loaded_annotations[aname] = Annotation(name=aname, datadict=adata)
    return loaded_annotations


loaded_annotations = load_mongo_annotations()

def load_mongo_data_information():
    all_sourceurl = set()
    for MongoNode in (GenomeNodes, InfoNodes, EdgeNodes):
        for sourceurl in MongoNode.distinct('sourceurl'):
            all_sourceurl.add(sourceurl)
    track_info = []
    for sourceurl in all_sourceurl:
        if 'GRCh38_latest_genomic' in sourceurl:
            track_info.append({
                'track_type': 'GRCh38_gff',
                'title': 'Genomic Elements',
                'description': 'Genes, Promoters, Enhancers, Binding Sites and more.'
            })
        elif 'gwas' in sourceurl:
            track_info.append({
                'track_type': 'gwas',
                'title': 'Genome Wide Associations',
                'description': 'Variants related to traits or diseases.'
            })
        elif 'exSNP' in sourceurl:
            track_info.append({
                'track_type': 'eqtl',
                'title': 'Quantitative Trait Loci',
                'description': 'Variants related to changes in gene expression or other quantitative measures.'
            })
    return track_info

loaded_track_info = load_mongo_data_information()
