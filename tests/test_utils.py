from scale_aware_air_sea.utils import to_zarr_split, weighted_coarsen
import dask.array as dsa
import xarray as xr
import json
import pytest

def dataset():
    nt = 50
    data = dsa.random.random((200, 1000, nt), chunks=(200, 1000, 1))
    ds_test = xr.Dataset({k:xr.DataArray(data, dims=['x','y','time'], coords={'time':range(nt)}) for k in ['a','b']})
    return ds_test

def test_roundtrip(tmp_path):
    d = tmp_path / "sub"
    d.mkdir()
    store = d / "test.zarr"
    
    ds = dataset()
    to_zarr_split(ds, store)
    
    ds_reloaded = xr.open_dataset(store, engine='zarr', chunks={})
    
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
    with open(str(store)+'/.zmetadata') as f:
        meta = json.load(f)
    with open(str(store_split)+'/.zmetadata') as f:
        meta_split = json.load(f)
        
    assert meta == meta_split
    
def test_simple_2_x_2():
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

def test_nan_mismatch_variables():
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
    

def test_nan_mismatch_weights():
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

def test_weights_nan():
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
        

def test_preserve_integral():
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
    
    
    
    