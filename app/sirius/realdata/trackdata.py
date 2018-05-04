from sirius.realdata.constants import QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE
from sirius.core.QueryTree import QueryTree
from sirius.mongo import InfoNodes
from sirius.realdata.constants import TILE_DB_PATH
import tiledb
import os
import math
import json

def read_track_data(track_id, chromosomeIdx, start_bp, end_bp, track_height_px, sampling_rate):
  # load the information on the track:
  trackInfo = InfoNodes.find_one({'_id': track_id})
  chr_list= trackInfo["info"]["chromosomes"]
  isSequence = trackInfo["type"] == "sequence"
  if (chromosomeIdx >= len(chr_list)):
    return json.dumps({
        "chromosomeIdx": chromosomeIdx,
        "startBp" : start_bp,
        "endBp" : end_bp,
        "samplingRate": sampling_rate,
        "numSamples": 0,
        "trackHeightPx": track_height_px,
        "values": None,
        "dimensions": [],
        "dataType": 'basepairs'
    })

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
    ret = db[start_bp - 1 : end_bp - 1]['value']
    if isSequence:
      track_data_type = 'basepairs'
      ret = map(lambda x : char_map[x], ret)
    else:
      track_data_type = 'value'
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
      datasets.append(db[int(math.floor(start_bp / resolution)):int(math.floor(end_bp / resolution)) - 1][dim])
    
    for i in range(0, num_samples):
      for dataset in datasets:
        idx = int((i / len(dataset)) * (len(dataset) - 1))
        ret.append(float(dataset[idx]))

  return json.dumps({
      "chromosomeIdx": chromosomeIdx,
      "startBp" : start_bp,
      "endBp" : end_bp,
      "samplingRate": sampling_rate,
      "numSamples": num_samples,
      "trackHeightPx": track_height_px,
      "values": ret,
      "dimensions": dimensions,
      "dataType": track_data_type
  })
