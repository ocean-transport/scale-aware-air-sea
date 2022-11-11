import gcm_filters
import xarray as xr
import numpy as np
from tqdm.auto import tqdm
import zarr

def smooth_inputs(da:xr.DataArray, wet_mask:xr.DataArray, dims:list, filter_scale:int) -> xr.DataArray:
    """Smoothes input using gcm-filters"""
    input_filter = gcm_filters.Filter(
        filter_scale=filter_scale,
        dx_min=1,
        filter_shape=gcm_filters.FilterShape.GAUSSIAN,
        grid_type=gcm_filters.GridType.REGULAR_WITH_LAND,
        grid_vars={'wet_mask': wet_mask}
    )
    out = input_filter.apply(da, dims=dims)
    out.attrs['filter_scale'] = filter_scale
    return out

def smooth_inputs_dataset(ds:xr.Dataset, dims:list, filter_scale:int, timedim:str='time') -> xr.Dataset:
    """Wrapper that filters a whole dataset, generating a wet_mask from the nanmask of the first timestep (if time is present)."""
    ds_out = xr.Dataset()
    for var in ds.data_vars:
        da = ds[var]
        if timedim in da.dims:
            mask_da = da.isel({timedim:0})
        else:
            mask_da = da
            
        wet_mask = (~np.isnan(mask_da)).astype(int)
        ds_out[var] = smooth_inputs(da, wet_mask, dims, filter_scale)
    return ds_out

def scale_separation(ds, filter_scale, mask):
    ds_filtered = smooth_inputs_dataset(
        ds,
        ['yt_ocean', 'xt_ocean'],
        filter_scale
    )
    diff_filtered = ds_filtered.sel(smoothing='smooth_full')-ds_filtered.sel(smoothing=['smooth_all', 'smooth_tracer', 'smooth_vel'])
    diff_unfiltered = ds_filtered.sel(smoothing='smooth_full')-ds.sel(smoothing=['smooth_all', 'smooth_tracer', 'smooth_vel'])
    
    # assigne scale datasets
    ds_full=ds_filtered.sel(smoothing='smooth_full')
    
    ds_large_scale = ds_filtered.sel(smoothing='smooth_all')
    
    ds_small_scale = xr.concat(
        [
            diff_unfiltered.sel(smoothing='smooth_all'), # the main result,
            diff_filtered.sel(smoothing=['smooth_tracer', 'smooth_vel']), # The mechanism 'hints'
        ],
        dim='smoothing'
    )
    
    # mask the outputs
    ds_full = ds_full.where(mask)
    ds_large_scale = ds_large_scale.where(mask)
    ds_small_scale = ds_small_scale.where(mask)
    
    return ds_full, ds_large_scale, ds_small_scale