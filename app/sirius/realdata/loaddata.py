import os
import json
from sirius.core.Annotation import Annotation
from sirius.realdata.constants import chromo_idxs


def load_mongo_annotations():
    from sirius.mongo import GenomeNodes
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
