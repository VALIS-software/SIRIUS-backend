import os, json, math
import tiledb

from sirius.mongo import InfoNodes
from sirius.helpers.loaddata import loaded_data_track_info_dict

def read_track_data(track_id, contig, start_bp, end_bp, track_height_px, sampling_rate):
    # load the information on the track:
    trackInfo = loaded_data_track_info_dict.get(track_id, None)
    if trackInfo == None:
        return json.dumps({
            "contig": contig,
            "startBp" : start_bp,
            "endBp" : end_bp,
            "samplingRate": sampling_rate,
            "numSamples": 0,
            "trackHeightPx": track_height_px,
            "values": [],
            "dimensions": [],
            "dataType": 'basepairs'
        })
    chr_list= trackInfo["info"]["contigs"]
    isSequence = trackInfo["type"] == "sequence"

    chr_info = chr_list[chromosomeIdx]

    resolutions = chr_info["resolutions"]
    tileServerId = chr_info["tileServerId"]

    best = 0
    for i, res in enumerate(resolutions):
        if res <= sampling_rate:
            best = i

    char_map = {
        b'n': 0.0,
        b'a': 0.25,
        b't': 0.5,
        b'c': 0.75,
        b'g': 1.0,
    }

    if best == 0:
        os.chdir(TILE_DB_PATH)
        ctx = tiledb.Ctx()
        db = tiledb.DenseArray.load(ctx, tileServerId)
        start_bp = max([start_bp, 1])
        end_bp = min([end_bp, len(db)])
        num_samples = end_bp - start_bp
        ret =  db[start_bp - 1 : end_bp - 1]['value']
        if isSequence:
            track_data_type = 'basepairs'
            ret = [char_map[x.lower()] for x in ret]
            dimensions = ['value']
        else:
            ret = [float(x) for x in ret]
            track_data_type = 'value'
            dimensions = ['value']
    else:
        resolution = resolutions[best]
        # return gband data:
        os.chdir(TILE_DB_PATH)
        ctx = tiledb.Ctx()
        db = tiledb.DenseArray.load(ctx, tileServerId + "_" + str(resolution))
        num_samples = int(math.floor(end_bp / sampling_rate)) - int(math.floor(start_bp / sampling_rate))

        if isSequence:
            track_data_type = 'gbands'
            dimensions = ['gc']
        else:
            track_data_type = 'value'
            dimensions = ['min', 'max', 'avg']

        # downsample to exact sampling rate
        ret = []
        datasets = []
        for dim in dimensions:
            sz = len(db)
            startIdx = int(math.floor(start_bp / resolution))
            endIdx = int(math.floor(end_bp / resolution)) - 1

            # check bounds:
            if startIdx < 0:
                startIdx = 0

            if endIdx < 0:
                endIdx = 0

            if startIdx > sz - 1:
                startIdx = sz - 1

            if endIdx > sz - 1:
                endIdx = sz -1

            if endIdx < startIdx:
                endIdx = startIdx

            datasets.append(db[startIdx:endIdx][dim])

        if startIdx != endIdx:
            for i in range(0, num_samples):
                for dataset in datasets:
                    idx = int((i / num_samples) * (len(dataset) - 1))
                    ret.append(float(dataset[idx]))
        else:
            ret = []
    response = {
        "chromosomeIdx": chromosomeIdx,
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "numSamples": num_samples,
        "trackHeightPx": track_height_px,
        "values": ret,
        "dimensions": dimensions,
        "dataType": track_data_type
    }
    return json.dumps(response)
