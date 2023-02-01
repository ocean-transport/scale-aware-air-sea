import json

import dask.array as dsa
import xarray as xr

from scale_aware_air_sea.utils import to_zarr_split


def dataset():
    nt = 50
    data = dsa.random.random((200, 1000, nt), chunks=(200, 1000, 1))
    ds_test = xr.Dataset(
        {
            k: xr.DataArray(data, dims=["x", "y", "time"], coords={"time": range(nt)})
            for k in ["a", "b"]
        }
    )
    return ds_test


def test_roundtrip(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    store = d / "test.zarr"

    ds = dataset()
    to_zarr_split(ds, store)

    ds_reloaded = xr.open_dataset(store, engine="zarr", chunks={})

    assert ds.chunks == ds_reloaded.chunks
    xr.testing.assert_equal(ds, ds_reloaded)


def test_metadata(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()

    store = d / "test.zarr"
    store_split = d / "test_split.zarr"

    ds = dataset()

    ds.to_zarr(store, consolidated=True)
    to_zarr_split(ds, store_split)

    # reload metadata files
    with open(str(store) + "/.zmetadata") as f:
        meta = json.load(f)
    with open(str(store_split) + "/.zmetadata") as f:
        meta_split = json.load(f)

    assert meta == meta_split
