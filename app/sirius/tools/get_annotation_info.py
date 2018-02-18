#!/usr/bin/env python

from sirius.realdata.constants import chromo_names

def get_annotation_info(GenomeNodes, annotationId):
    chromo_info = dict()
    for d in GenomeNodes.find({'type': 'region', 'assembly': annotationId, 'info.attributes.genome': 'chromosome'}):
        ch = d['location']
        chromo_info[ch] = {'start': d['start'], 'end': d['end'], 'seqid': d['info']['seqid']}
    if not chromo_info:
        print("AnnotationID %s not found in %s" % (annotationId, GenomeNodes.name))
        return
    chromo_lengths = [chromo_info[ch]['end'] - chromo_info[ch]['start'] + 1 for ch in chromo_names]
    seqids = [chromo_info[ch]['seqid'] for ch in chromo_names]
    start_bp = chromo_info[chromo_names[0]]['start']
    end_bp = sum(chromo_lengths) + start_bp - 1
    info = {annotationId: {'start_bp': start_bp, 'end_bp': end_bp, 'chromo_lengths': chromo_lengths, 'seqids': seqids}}
    return info


def main():
    from sirius.mongo import GenomeNodes
    import argparse, json
    parser = argparse.ArgumentParser()
    parser.add_argument("annotationId")
    parser.add_argument("-o", '--out_json', help='Output Json filename')
    args = parser.parse_args()

    if args.out_json == None:
        args.out_json = args.annotationId + '.json'
    info = get_annotation_info(GenomeNodes, args.annotationId)
    with open(args.out_json, 'w') as outfile:
        json.dump(info, outfile, indent=2)

if __name__ == "__main__":
    main()
