import json
import pytest

import dask.array as dsa
import numpy as np
import xarray as xr

from scale_aware_air_sea.utils import to_zarr_split, weighted_coarsen


def dataset():
    nt = 50
    nx, ny = 20, 100
    data = dsa.random.random((nx, ny, nt), chunks=(nx, ny, 1))
    x = xr.DataArray(np.linspace(-10, 10, nx), dims=['x'])
    y = xr.DataArray(np.linspace(20, 120, ny), dims=['y'])
    lon = x * xr.ones_like(y)
    lat = xr.ones_like(x) * y
    ds_test = xr.Dataset(
        {
            k: xr.DataArray(data, dims=["x", "y", "time"], coords={"time": range(nt), 'x':x, 'y':y, 'lon':lon, 'lat':lat})
            for k in ["a", "b"]
        }, 
    )
    return ds_test

class Test_to_zarr_split:
    def test_roundtrip(self, tmp_path):
        d = tmp_path / "sub"
        d.mkdir()
        store = d / "test.zarr"

        ds = dataset()
        to_zarr_split(ds, store)

        ds_reloaded = xr.open_dataset(store, engine="zarr", chunks={})

        assert ds.chunks == ds_reloaded.chunks
        xr.testing.assert_equal(ds, ds_reloaded)


    def test_metadata(self, tmp_path):
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
        
class Test_weighted_coarsen:
    def test_simple_2_x_2(self):
        data_full = np.random.rand(4,4)
        weights_full = np.random.rand(4,4)

        d = data_full * weights_full

        weights_expected = np.hstack([
            np.vstack([weights_full[0:2, 0:2].sum(), weights_full[2:5, 0:2].sum()]),
            np.vstack([weights_full[0:2, 2:5].sum(), weights_full[2:5, 2:5].sum()]),
        ])

        data_expected = np.hstack([
            np.vstack([d[0:2, 0:2].sum(), d[2:5, 0:2].sum()]),
            np.vstack([d[0:2, 2:5].sum(), d[2:5, 2:5].sum()]),
        ]) / weights_expected

        ds = xr.DataArray(data_full, coords={'area':(['x','y'],weights_full)}, dims=['x','y']).to_dataset(name='data')

        da_coarse = weighted_coarsen(ds, {'x':2, 'y':2}, 'area')

        np.testing.assert_allclose(da_coarse.data, data_expected)
        np.testing.assert_allclose(da_coarse.area, weights_expected)

    def test_nan_mismatch_variables(self):
        data_full = np.random.rand(4,4)
        data_2_full = np.random.rand(4,4)
        data_2_full[0,1] = np.nan

        weights_full = np.random.rand(4,4)
        ds = xr.Dataset({
            'data1':xr.DataArray(data_full, dims=['x','y']),
            'data2':xr.DataArray(data_2_full, dims=['x','y']),
        }, 
            coords={'area':(['x','y'],weights_full)},
        )
        with pytest.raises(ValueError, match='Found variables with non-matching missing values.'):
            weighted_coarsen(ds, {'x':2, 'y':2}, 'area')


    def test_nan_mismatch_weights(self):
        data_full = np.random.rand(4,4)
        data_full[0,1] = np.nan
        data_2_full = np.random.rand(4,4)
        data_2_full[0,1] = np.nan

        weights_full = np.random.rand(4,4)
        ds = xr.Dataset({
            'data1':xr.DataArray(data_full, dims=['x','y']),
            'data2':xr.DataArray(data_2_full, dims=['x','y']),
        }, 
            coords={'area':(['x','y'],weights_full)},
        )
        with pytest.raises(ValueError, match='Missing values in variables are not matching locations of <=0 values in weights array.'):
            weighted_coarsen(ds, {'x':2, 'y':2}, 'area')

    def test_weights_nan(self):
        data_full = np.random.rand(4,4)
        data_2_full = np.random.rand(4,4)

        weights_full = np.random.rand(4,4)
        weights_full[0,1] = np.nan

        ds = xr.Dataset({
            'data1':xr.DataArray(data_full, dims=['x','y']),
            'data2':xr.DataArray(data_2_full, dims=['x','y']),
        }, 
            coords={'area':(['x','y'],weights_full)},
        )
        with pytest.raises(ValueError, match='Found missing values in weights coordinate '):
            weighted_coarsen(ds, {'x':2, 'y':2}, 'area')


    def test_preserve_integral(self):
        data_full = np.random.rand(4,4, 10)
        data_2_full = np.random.rand(4,4, 10)

        weights_full = np.random.rand(4,4)

        ds = xr.Dataset({
            'data1':xr.DataArray(data_full, dims=['x','y', 'time']),
            'data2':xr.DataArray(data_2_full, dims=['x','y', 'time']),
        }, 
            coords={'area':(['x','y'],weights_full)},
        )

        ds_coarse = weighted_coarsen(ds, {'x':2, 'y':2}, 'area')

        # We expect the weighted mean of both the original and coarsened dataset to stay the same
        mean_fine = ds.weighted(ds.area).mean(['x','y'])
        mean_coarse = ds_coarse.weighted(ds_coarse.area).mean(['x','y'])

        xr.testing.assert_allclose(mean_fine, mean_coarse)
        
    def test_1d_coords(self):
        ds = dataset()
        ds['area'] = xr.ones_like(ds.a.isel(time=0))
        
        ds_coarse = weighted_coarsen(ds, {'x':2, 'y':4}, 'area')
        
        x_expected = ds.x.coarsen({'x':2}).mean()
        y_expected = ds.y.coarsen({'y':4}).mean()
        
        xr.testing.assert_equal(x_expected, ds_coarse.x)
        xr.testing.assert_equal(y_expected, ds_coarse.y)
    
    def test_2d_coords(self):
        ds = dataset()
        ds['area'] = xr.ones_like(ds.a.isel(time=0))
        
        ds_coarse = weighted_coarsen(ds, {'x':2, 'y':4}, 'area')
        
        lon_expected = ds.reset_coords().lon.coarsen({'x':2, 'y':4}).mean()
        lat_expected = ds.reset_coords().lat.coarsen({'x':2, 'y':4}).mean()
        
        xr.testing.assert_allclose(lon_expected, ds_coarse.reset_coords().lon)
        xr.testing.assert_allclose(lat_expected, ds_coarse.reset_coords().lat)
        
        
