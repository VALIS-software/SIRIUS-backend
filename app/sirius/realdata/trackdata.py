from sirius.realdata.constants import TILE_DB_PATH
import tiledb

def get_chromosome(track_info, chromosome_name):
  for chro in track_info["info"]["chromosomes"]:
    if chro["name"] == chromosome:
      return chro

def get_fasta_data(track_id, chromosome, start_bp, end_bp, track_data_type, track_height_px, sampling_rate):
  # load the information on the track:
  qt = QueryTree({
      "type": QUERY_TYPE_INFO,
      "filters": { "_id" : track_id},
      "toEdges": []    
  })
  chr_info = get_chromosome(qt.find()[0], chromosome_name)

  resolutions = chr_info["resolutions"]
  tileServerId = chr_info["tileServerId"]

  best = 0
  for i, res in enumerate(resolutions):
    if res <= sampling_rate:
      best = i

  if best == 0:
    # return raw sequence data:
    os.chdir(TILE_DB_PATH)
    ctx = tiledb.Ctx()
    db = tiledb.DenseArray.load(ctx, tileServerId)
    return db[start_bp - 1 : end_bp - 1]
  else:
    resolution = resolutions[best]
    # return gband data:
    os.chdir(TILE_DB_PATH)
    ctx = tiledb.Ctx()
    db = tiledb.DenseArray.load(ctx, tileServerId + "_" + str(resolution))
    return db[start_bp / resolution:end_bp / resolution]

def get_bigwig_data(track_id, chromosome, start_bp, end_bp, track_data_type, track_height_px, sampling_rate, aggregations):
  return None