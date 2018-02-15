import os
import json

this_file_folder = os.path.dirname(os.path.realpath(__file__))

mockAnnotations = json.load(open(os.path.join(this_file_folder, "mockAnnotations.json")))

mockData = json.load(open(os.path.join(this_file_folder, "mockData.json")))

def getMockAnnotations():
    return mockAnnotations

def getMockData():
    return mockData
