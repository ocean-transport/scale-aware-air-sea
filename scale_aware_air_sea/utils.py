from tqdm.auto import tqdm
import xarray as xr
import zarr
import gcm_filters
import numpy as np

def smooth_inputs(da:xr.DataArray, wet_mask:xr.DataArray, dims:list, filter_scale:int, filter_type:str='taper') -> xr.DataArray:
    """Smoothes input using gcm-filters"""
    if filter_type == 'gaussian':
        input_filter = gcm_filters.Filter(
            filter_scale=filter_scale,
            dx_min=1,
            filter_shape=gcm_filters.FilterShape.GAUSSIAN,
            grid_type=gcm_filters.GridType.REGULAR_WITH_LAND,
            grid_vars={'wet_mask': wet_mask}
        )
    elif filter_type == 'taper':
        # transition_width = np.pi/2
        transition_width = np.pi
        input_filter = gcm_filters.Filter(
            filter_scale=filter_scale,
            transition_width=transition_width,
            dx_min=1,
            filter_shape=gcm_filters.FilterShape.TAPER,
            grid_type=gcm_filters.GridType.REGULAR_WITH_LAND,
            grid_vars={'wet_mask': wet_mask}
        )
        
    else:
        raise ValueError(f"`filter_type` needs to be `gaussian` or `taper', got {filter_type}")
    out = input_filter.apply(da, dims=dims)
    out.attrs['filter_scale'] = filter_scale
    out.attrs['filter_type'] = filter_type
    return out

def smooth_inputs_dataset(ds:xr.Dataset, dims:list, filter_scale:int, timedim:str='time', filter_type:str='taper') -> xr.Dataset:
    """Wrapper that filters a whole dataset, generating a wet_mask from the nanmask of the first timestep (if time is present)."""
    ds_out = xr.Dataset()
    
    # create a wet mask that only allows values which are 'wet' in all variables
    
    
    wet_masks = []
    for var in ds.data_vars:
        da = ds[var]
        if timedim in da.dims:
            mask_da = da.isel({timedim:0})
        else:
            mask_da = da
        wet_masks.append((~np.isnan(mask_da)))
                         
    combined_wet_mask = xr.concat(wet_masks, dim='var').all('var').astype(int)
    
    for var in ds.data_vars:
        ds_out[var] = smooth_inputs(ds[var], combined_wet_mask, dims, filter_scale, filter_type=filter_type)
    
    ds_out.attrs['filter_scale'] = filter_scale
    ds_out.attrs['filter_type'] = filter_type
    return ds_out

def scale_separation(ds, filter_scale, mask):
    ds_filtered = smooth_inputs_dataset(
        ds,
        ['yt_ocean', 'xt_ocean'],
        filter_scale
    )
    
    all_smoothing_options_except_full = [s for s in ds.smoothing.data if 'full' not in s]
    
    
    diff_filtered = ds_filtered.sel(smoothing='smooth_full')-ds_filtered.sel(smoothing=all_smoothing_options_except_full)
    diff_unfiltered = ds_filtered.sel(smoothing='smooth_full')-ds.sel(smoothing=all_smoothing_options_except_full)
    
    # assigne scale datasets
    ds_full=ds_filtered.sel(smoothing='smooth_full')
    
    ds_large_scale = ds.sel(smoothing='smooth_all')
    
    
    ds_small_scale = xr.concat(
        [
            diff_unfiltered.sel(smoothing='smooth_all'), # the main result,
            diff_filtered.sel(smoothing=[s for s in diff_filtered.smoothing.data if 'all' not in s]), # The mechanism 'hints'
        ],
        dim='smoothing'
    )
    
    # mask the outputs
    ds_full = ds_full.where(mask)
    ds_large_scale = ds_large_scale.where(mask)
    ds_small_scale = ds_small_scale.where(mask)
    
    return ds_full, ds_large_scale, ds_small_scale


def to_zarr_split(ds, mapper, split_dim='time', split_interval=1):
    #
#     if mapper.fs.exists(mapper.root):
#         raise ValueError(f'{mapper.root} already exists. Please delete manually before writing')
    print(f'Writing to {mapper.root} ...')
    
    n = len(ds[split_dim])
    splits = list(range(0,n,split_interval))

    # Make sure the last item in the list covers the full length of the time on our dataset
    if splits[-1] != n:
        splits = splits + [n]
        
    split_datasets = []
    for ii in range(len(splits)-1):
        start = splits[ii]
        stop = splits[ii+1]
        split_datasets.append(ds.isel({split_dim:slice(start, stop)}))
    
    # write the first array
    # TODO: move the first write to the loop so it is counted in the viz bar
    split_datasets[0].to_zarr(mapper)
    for ds_split in tqdm(split_datasets[1:None]):
        ds_split.to_zarr(mapper, append_dim=split_dim)
        
    # overwrite the split dimension as single chunk (this should reproduce
    # what xr.to_zarr would do
    g = zarr.open_group(mapper)
    del g[split_dim]
    
    ds[[split_dim]].load().to_zarr(mapper, mode='a')
    zarr.consolidate_metadata(mapper)