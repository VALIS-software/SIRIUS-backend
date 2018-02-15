import os
import json

this_file_folder = os.path.dirname(os.path.realpath(__file__))

realAnnotations = json.load(open(os.path.join(this_file_folder, "realAnnotations.json")))

realData = json.load(open(os.path.join(this_file_folder, "realData.json")))
