# coding: utf-8
from sirius.realdata.loaddata import loaded_annotations
a = loaded_annotations['GRCh38']
result = a.db_find('gene', 213121, 912301, 12333, verbose=True)
print(next(result))
