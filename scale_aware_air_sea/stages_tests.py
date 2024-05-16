import xarray as xr
import random

def _test_timesteps(ds:xr.Dataset):
    assert 'model' in ds.attrs
    prod_spec = ds.attrs.get('production_spec')
    if prod_spec == 'appendix':
        assert len(ds.time) == 365
    elif prod_spec == 'prod':
        if ds.attrs['model'] == 'CESM':
            assert len(ds.time) == 730
        elif ds.attrs['model'] == 'CM26':
            assert len(ds.time) == 7305

def test_data_preprocessing(ds:xr.Dataset, full_check=False):
    # check that no nans are in the lon/lat fields (warn only, I think we do not have any without nans atm)
    # Note. Actually this might be useful for the regridding (and masking within), but we should be able to attache fully filled lon/lats for later plotting.
    _test_timesteps(ds)
    # check that all variables are on the tracer point
    for va in ds.data_vars:
        assert set(ds[va].dims) == set(['time', 'xt_ocean', 'yt_ocean'])
    
    # check that necessary coordinates are included
    for co in ['ice_mask', 'area_t', 'geolon_t', 'geolat_t']:
        assert co in ds.coords
        
    # Range check on naive global mean for variables
    ranges = {
        'surface_temp':[270, 310],
        'q_ref':[0.005, 0.02],
        'slp': [100000, 110000],
        't_ref': [270, 310],
        'u_ocean': [0.05, 2],
        'v_ocean': [0.05, 2],
        'u_relative' : [1, 20],
        'v_relative' : [1, 20],
        'u_ref' : [1, 20], 
        'v_ref' : [1, 20],
        'area_t': [5e7, 2e8],
    }
    range_test_ds = ds.isel(time=random.randint(0, len(ds.time))).load()
    for va, r in ranges.items():
        test_val = abs(range_test_ds[va]).quantile(0.75).data
        if not (test_val >= r[0] and test_val <= r[1]):
            raise ValueError(f"{va =} failed the range test. Got value={test_val} and range={r}")
    
    # TODO: Check the proper units?

    if full_check:
        # test that there are no all nan maps anywhere
        nan_test = np.isnan(ds).all(['xt_ocean', 'yt_ocean']).to_array().sum()
        assert nan_test.data == 0
        
        
    # finally check that each variable has nans in the same position
    a = np.isnan(ds.surface_temp.isel(time=0, drop=True)).load()
    b = np.isnan(ds.isel(time=0, drop=True).to_array()).all('variable').load()
    xr.testing.assert_allclose(a,b)

    
    
import matplotlib.pyplot as plt
import numpy as np

def test_smoothed_data(ds_raw, ds, plot=False, full_check=False):
    _test_timesteps(ds)
    assert 'smoothing_method' in ds.attrs.keys()
    
    if ds.attrs['smoothing_method'] == 'coarse':
        assert 'n_coarsen' in ds.attrs.keys()
        
        # Test that raw and coarse datasets preserver the global mean tracer value
        # This ensures that both the values and the coarsened area are calculated consistently
        test_var = 'surface_temp'
        test_roi = dict(time=slice(0,200))
        # FIXME: THERE IS THIS BIZARRE precision error with weighted again...WTF. Take the `.astype(...)` out to see this mess!!!
        raw_test = ds_raw[test_var].isel(**test_roi).astype(np.float64).weighted(ds_raw.area_t).mean(['xt_ocean', 'yt_ocean']).load()
        test = ds[test_var].isel(**test_roi).astype(np.float64).weighted(ds.area_t).mean(['xt_ocean', 'yt_ocean']).load()
        if plot:
            plt.figure()
            raw_test.plot(label='raw', ls='-')
            test.plot(label='coarse', ls=':')
            plt.title(f'Global weighted {test_var} average {model}')
            plt.legend()
            plt.show()
        xr.testing.assert_allclose(raw_test, test)
    
    elif ds.attrs['smoothing_method'] == 'filter':
        assert 'filter_type' in ds.attrs.keys()
        assert 'filter_scale' in ds.attrs.keys()

    if full_check:
        # test that there are no all nan maps anywhere
        nan_test = np.isnan(ds).all(['xt_ocean', 'yt_ocean']).to_array().sum()
        assert nan_test.data == 0
      
    ## Tests for all smoothed datasets
    ## are eddies visually eliminated?
    if plot:
        plt.figure()
        ds.isel(time=[0, 100, 300]).surface_temp.plot.contourf(col='time', levels=21, size=4)
        plt.show()

def test_data_flux(ds:xr.Dataset, plot=False, full_check=False):
    for attr in ['smoothing_method', 'production_spec', 'model']:
        print(attr)
        assert attr in ds.attrs.keys()
    _test_timesteps(ds)

    # test that there are no all nan maps anywhere
    if full_check:
        nan_test = np.isnan(ds).all(['xt_ocean', 'yt_ocean']).to_array().sum()
        assert nan_test.data == 0

    # Check the ice-mask
    if plot:
        plt.figure()
        ds.qh.isel(time=[0,90, 180], algo=0, smoothing=0).plot(col='time', robust=True)
        plt.show()