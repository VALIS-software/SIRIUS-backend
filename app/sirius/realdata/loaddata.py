import os, json
from sirius.core.Annotation import Annotation
from sirius.realdata.constants import CHROMO_IDXS
from sirius.mongo import GenomeNodes, InfoNodes, Edges

this_file_folder = os.path.dirname(os.path.realpath(__file__))

# we load from the json file on disk
def load_annotations():
    loaded_annotations = dict()
    with open(os.path.join(this_file_folder, 'realAnnotations.json')) as jsonin:
        json_data = json.load(jsonin)
        for aname, adata in json_data.items():
            loaded_annotations[aname] = Annotation(name=aname, datadict=adata)
    return loaded_annotations

loaded_annotations = load_annotations()

def load_mongo_data_information():
    from sirius.realdata.constants import DATA_SOURCE_GENOME, DATA_SOURCE_GWAS, DATA_SOURCE_EQTL, DATA_SOURCE_CLINVAR, DATA_SOURCE_DBSNP, DATA_SOURCE_ENCODE
    from sirius.realdata.constants import TRACK_TYPE_GENOME, TRACK_TYPE_GWAS, TRACK_TYPE_EQTL, TRACK_TYPE_ENCODE
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
    track_info = []
    for t in track_type_list:
        if any(d in loaded_dataSources for d in t.pop('depends')):
            track_info.append(t)
    return track_info

loaded_track_info = load_mongo_data_information()
