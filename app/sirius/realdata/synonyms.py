#!/usr/bin/env python

class KeyDict(dict):
    def __missing__(key):
        return key

Synonyms = KeyDict({
    'hg19': 'GRCh37'
})
