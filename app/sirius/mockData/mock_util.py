import os
import json

this_file_folder = os.path.dirname(os.path.realpath(__file__))

with open(os.path.join(this_file_folder, "mockAnnotations.json")) as infile:
    mockAnnotations = json.load(infile)

with open(os.path.join(this_file_folder, "mockData.json")) as infile:
    mockData = json.load(infile)

def getMockAnnotations():
    return mockAnnotations

def getMockData():
    return mockData
