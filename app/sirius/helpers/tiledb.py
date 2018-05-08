import numpy as np
import os
import tiledb

class TileHelper(object):
    """
    The TileHelper class for convenient tiledb setup
    """
    config = tiledb.Config()
    config["vfs.s3.scheme"] = "https"
    config["vfs.s3.region"] = "us-west-1"
    config["vfs.s3.use_virtual_addressing"] = "true"
    ctx = tiledb.Ctx(config)

    def __init__(self, backend="local", tile_size=1000000):
        if backend == 'local':
            self.root = '/pd/tiledb/'
        elif backend == 's3':
            self.root = 's3://sirius-tiledb/'
        else:
            raise NotImplementedError("backend can be 's3' or 'local'")
        self.backend = backend
        self.tile_size = tile_size

    def create_dense_array(self, arrayID, data):
        assert isinstance(data, np.ndarray), "data should be an np.ndarray"
        tile_dims = []
        for i_dim, dim_size in enumerate(data.shape):
            name = f'd{i_dim}'
            tile = min(self.tile_size, dim_size)
            tiledim = tiledb.Dim(self.ctx, name=name, domain=(0, dim_size-1), tile=tile)
            tile_dims.append(tiledim)
        domain = tiledb.Domain(self.ctx, *tile_dims)
        #print(domain.dump())
        attr = tiledb.Attr(self.ctx, "value", dtype=data.dtype)
        tile_array_id = self.root + arrayID
        dense_array = tiledb.DenseArray(self.ctx, tile_array_id, domain=domain, attrs=[attr])
        dense_array[:] = data
        return dense_array

    def load_dense_array(self, arrayID):
        tile_array_id = self.root + arrayID
        return tiledb.DenseArray.load(self.ctx, tile_array_id)

    def remove(self, arrayID):
        tile_array_id = self.root + arrayID
        tiledb.remove(self.ctx, tile_array_id)

    def ls(self):
        paths = []
        tiledb.ls(self.ctx, self.root, lambda p,l: paths.append(p))
        if self.backend == "s3":
            results = [ p[len(self.root):-1] for p in paths ]
        else:
            results = [ os.path.basename(p) for p in paths ]
        return results

tilehelper = TileHelper()

