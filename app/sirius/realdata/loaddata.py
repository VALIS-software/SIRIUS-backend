import os
import json
from sirius.core.Annotation import Annotation
from sirius.realdata.constants import chromo_idxs, DATA_SOURCE_GENOME, DATA_SOURCE_GWAS, DATA_SOURCE_EQTL, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP
from sirius.mongo import GenomeNodes, InfoNodes, Edges

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
    track_info = []
    # the description is hard coded here, could be replaced by querying InfoNodes of type dataSource later
    for source in InfoNodes.distinct('source'):
        if source == DATA_SOURCE_GENOME:
            track_info.append({
                'track_type': DATA_SOURCE_GENOME,
                'title': 'Genomic Elements',
                'description': 'Genes, Promoters, Enhancers, Binding Sites and more.'
            })
        elif source == DATA_SOURCE_GWAS:
            track_info.append({
                'track_type': DATA_SOURCE_GWAS,
                'title': 'Genome Wide Associations',
                'description': 'Variants related to traits or diseases.'
            })
        elif source == DATA_SOURCE_EQTL:
            track_info.append({
                'track_type': DATA_SOURCE_EQTL,
                'title': 'Quantitative Trait Loci',
                'description': 'Variants related to changes in gene expression or other quantitative measures.'
            })
        elif source == DATA_SOURCE_CLINVAR:
            track_info.append({
                'track_type': DATA_SOURCE_CLINVAR,
                'title': 'ClinVar',
                'description': 'Variants related to phenotypes.'
            })
        elif source == DATA_SOURCE_DBSNP:
            track_info.append({
                'track_type': DATA_SOURCE_DBSNP,
                'title': 'dbSNP',
                'description': 'the NCBI database of genetic variation.'
            })
        else:
            print("Warning, data source %s is not recognized" % source)

    return track_info

loaded_track_info = load_mongo_data_information()
