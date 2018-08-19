import os, copy
from sirius.parsers.parser import Parser
from sirius.parsers.liftover import lo
from sirius.helpers import KeyDict
from sirius.helpers.constants import DATA_SOURCE_23ANDME, KNOWN_CONTIGS

class TxtParser(Parser):
    def parse(self):
        """ parse a txt file separated by tab into docs """
        # start from the beginning for reading
        self.filehandle.seek(0)
        assert self.parse_chunk(size=-1) == True, 'self.parse_chunk() did not finish parsing the entire file.'

    def parse_chunk(self, size=100000):
        self.entries = []
        for line in self.filehandle:
            line = line.strip() # remove '\n'
            if line and line[0] != '#':
                self.entries.append(line.split())
                if self.verbose and len(self.entries) % 100000 == 0:
                    print("%d data parsed" % len(self.entries), end='\r')
                if len(self.entries) == size:
                    if self.verbose:
                        print(f"Parsing file {self.filename} finished for chunk of size {size}" )
                    break
        else:
            if self.verbose:
                print(f"Parsing the entire file {self.filename} finished.")
            return True
        return False

class TxtParser_23andme(TxtParser):
    genotype_alt = KeyDict([(a+b, a+','+b) for a in 'ACGT' for b in 'ACGT'] + [('--', '')])
    def get_mongo_nodes(self):
        genome_nodes, info_nodes, edges = [], [], []
        for d in self.entries:
            rsid, contig_idx, position,	genotype = d
            if rsid[:2] == 'rs':
                gid = 'Gsnp_' + rsid
                contig = 'chr' + contig_idx
                pos = int(position)
                # liftover GRCh37 to GRCh38
                lo_result = lo.convert_coordinate(contig, pos, '+')
                if not lo_result:
                    continue
                # here we replace contig and position, but leave the others unchanged
                contig, pos, _, _ = lo_result[0]
                # skip unknown contigs
                if contig not in KNOWN_CONTIGS:
                    continue
                # the alt is parsed from genotype
                alt = self.genotype_alt[genotype]
                gnode = {
                    '_id': gid,
                    'source': DATA_SOURCE_23ANDME,
                    'type': 'SNP',
                    'name': rsid,
                    'contig': contig,
                    'start': pos,
                    'end':pos,
                    'length': 1,
                    'info': {
                        'variant_ref': 'N',
                        'variant_alt': alt
                    }
                }
                genome_nodes.append(gnode)
        return genome_nodes, info_nodes, edges