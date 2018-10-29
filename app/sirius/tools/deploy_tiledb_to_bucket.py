import os
import subprocess

tiledb_root = os.environ.get('TILEDB_ROOT', None)
if tiledb_root != None:
    # if os.path.isdir(tiledb_root):
    #     os.chdir(tiledb_root)
    #     print("Compressing tiledb folders")
    #     subprocess.check_call("tar zvcf sirius_fasta_data.tar.gz fasta*/", shell=True)
    #     print("Uploading to Cloud Storage Bucket")
    #     subprocess.check_call("gsutil cp sirius_fasta_data.tar.gz gs://siriusdata", shell=True)
    # else:
    #     print(f"TileDB root folder {tiledb_root} does not exist, will do nothing here")
    outstr = subprocess.check_output("gsutil ls gs://siriusdata/tiledb", shell=True)
    if outstr.startswith("CommandException"):
        subprocess.run(f"gsutil -m cp -r {tiledb_root} gs://siriusdata/tiledb", shell=True, check=True)
    else:
        subprocess.run(f"gsutil -m rsync -r -d gs://siriusdata/tiledb {tiledb_root}", shell=True, check=True)
else:
    print("TILEDB_ROOT is not set. Will do nothing.")
