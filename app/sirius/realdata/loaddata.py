import os
import json
from sirius.core.Annotation import Annotation
from sirius.realdata.constants import chromo_names


#def load_local_annotations():
#    this_file_folder = os.path.dirname(os.path.realpath(__file__))
#    realAnnotations = json.load(open(os.path.join(this_file_folder, "realAnnotations.json")))
#    realData = json.load(open(os.path.join(this_file_folder, "realData.json")))
#
#    loaded_annotations = dict()
#    for aname, adata in realAnnotations.items():
#        loaded_annotations[aname] = Annotation(name=aname, datadict=adata)
#
#    return loaded_annotations


def load_mongo_annotations():
    from sirius.mongo import GenomeNodes
    anno_names = GenomeNodes.distinct('assembly')
    loaded_annotations = dict()
    for aname in anno_names:
        chromo_info = dict()
        for d in GenomeNodes.find({'type': 'region', 'assembly': aname, 'info.attributes.genome': 'chromosome'}):
            ch = d['location']
            chromo_info[ch] = {'start': d['start'], 'end': d['end'], 'seqid': d['info']['seqid']}
        if not chromo_info:
            print("AnnotationID %s not found in %s" % (annotationId, GenomeNodes.name))
            return
        chromo_lengths = [chromo_info[ch]['end'] - chromo_info[ch]['start'] + 1 for ch in chromo_names]
        seqids = [chromo_info[ch]['seqid'] for ch in chromo_names]
        start_bp = chromo_info[chromo_names[0]]['start']
        end_bp = sum(chromo_lengths) + start_bp - 1
        adata = {'start_bp': start_bp, 'end_bp': end_bp, 'chromo_lengths': chromo_lengths, 'seqids': seqids}
        loaded_annotations[aname] = Annotation(name=aname, datadict=adata)
    return loaded_annotations


loaded_annotations = load_mongo_annotations()

