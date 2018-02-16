import os
import json
from sirius.core.Annotation import Annotation

def loaddata():
    this_file_folder = os.path.dirname(os.path.realpath(__file__))
    realAnnotations = json.load(open(os.path.join(this_file_folder, "realAnnotations.json")))
    realData = json.load(open(os.path.join(this_file_folder, "realData.json")))

    loaded_annotations = dict()
    for aname, adata in realAnnotations.items():
        loaded_annotations[aname] = Annotation(name=aname, datadict=adata)

    return loaded_annotations

loaded_annotations = loaddata()
