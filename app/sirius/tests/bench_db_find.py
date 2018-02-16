# coding: utf-8
from sirius.realdata.loaddata import loaded_annotations
import random
import argparse
import time

def random_query_one(anno):
    qlength = random.normalvariate(anno.length/23, anno.length/100)
    qlength = int(max(min(qlength,anno.length),1))
    startBp = random.randint(1, anno.length-qlength) + anno.start_bp - 1
    endBp = startBp + qlength - 1
    min_length = qlength // 200
    return anno.db_find(startBp, endBp, types=['gene'], min_length=min_length, verbose=True)

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-n', '--nquery', type=int, default=1, help='Repeat N random queries')
    args = parser.parse_args()

    print("Benchmarking db_find() of Annotation.")
    anno = loaded_annotations['GRCh38']

    t0 = time.time()
    for i in range(args.nquery):
        result = list(random_query_one(anno))
        print("Query %d Found %d" % (i, len(result)))

    t1 = time.time()
    print("%d random queries finished in %.2f seconds" % (args.nquery, t1-t0))


if __name__ == "__main__":
    main()
