from sirius.realdata.constants import QUERY_TYPE_GENOME, QUERY_TYPE_INFO, QUERY_TYPE_EDGE
from sirius.core.QueryTree import QueryTree
from sirius.realdata.constants import TILE_DB_PATH
import tiledb
import os
import math
import json

def get_fasta_data(track_id, chromosomeIdx, start_bp, end_bp, track_height_px, sampling_rate):
  # load the information on the track:
  qt = QueryTree({
      "type": QUERY_TYPE_INFO,
      "filters": { "_id" : track_id},
      "toEdges": []    
  })
  chr_list= qt.find()[0]["info"]["chromosomes"]
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


  if best == 0:
    track_data_type = 'basepairs'
    # return raw sequence data:
    os.chdir(TILE_DB_PATH)
    ctx = tiledb.Ctx()
    db = tiledb.DenseArray.load(ctx, tileServerId)
    start_bp = max([start_bp, 1])
    end_bp = min([end_bp, len(db)])
    chars = db[start_bp - 1 : end_bp - 1]['value']
    ret = []
    for char in chars:
      if char.lower() == b'n':
        ret.append(0.0)
      elif char.lower() == b'a':
        ret.append(0.25)
      elif char.lower() == b't':
        ret.append(0.5)
      elif char.lower() == b'c':
        ret.append(0.75)
      else:
        ret.append(1.0)
    # convert to float representation
    num_samples = end_bp - start_bp
    dimensions = ['value']
  else:
    track_data_type = 'gbands'
    resolution = resolutions[best]
    # return gband data:
    os.chdir(TILE_DB_PATH)
    ctx = tiledb.Ctx()
    db = tiledb.DenseArray.load(ctx, tileServerId + "_" + str(resolution))
    raw_data = db[int(math.floor(start_bp / resolution)):int(math.floor(end_bp / resolution)) - 1]["gc"]
    num_samples = int(math.floor(end_bp / sampling_rate)) - int(math.floor(start_bp / sampling_rate))
    dimensions = ['gc']
    # downsample to exact sampling rate
    ret = []
    for i in range(0, num_samples):
      idx = int((i / len(raw_data)) * (len(raw_data) - 1))
      ret.append(float(raw_data[idx]))
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

def get_bigwig_data(track_id, chromosomeIdx, start_bp, end_bp, track_data_type, track_height_px, sampling_rate, aggregations):
  return None