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



def write_split_zarr(store, ds, split_dim='time', chunks=1, split_interval=180):
    """Write a large dataset in a sequence of batches split along one of the dataset dimensions.
    This can be helpful to e.g. avoid problems with overly eager dask schedulers
    """
    # Got my inspiration for this mostly here: https://github.com/pydata/xarray/issues/6069
    
    # erase existing store
    if store.fs.exists(store.root):
        print(f'removing {store.root}')
        store.fs.rm(store.root, recursive=True)
    
    # determine the variables and coordinates that depend on the split_dim
    other_dims = [di for di in ds.dims if di != split_dim]
    split_vars_coords = [va for va in ds.variables if split_dim in ds[va].dims and va not in ds.dims]
    non_split_vars_coords = [va for va in ds.variables if va not in split_vars_coords and va not in ds.dims]
    
    # Generate a stripped dataset that only contains variables/coordinates that do not depend on `split_dim`
    ds_stripped = ds.drop_vars(split_vars_coords+[split_dim])
        
    # initialize the store without writing values
    print('initializing store')
    ds.to_zarr(
        store,
        compute=False,
        encoding={split_dim:{"chunks":[chunks]}},
        consolidated=True, # TODO: Not sure if this is proper. Might have to consolidate the whole thing as a last step?
    )
    
    # Write out only the non-split variables/coordinates
    if len(non_split_vars_coords) > 0:
        print('Writing coordinates')
        ds_stripped.to_zarr(store, mode='a') #I guess a is 'add'. This is honestly not clear enough in the xarray docs.
        # with `w` there are issues with the shape. 
    
    # TODO: what about the attrs?
    
    # Populate split chunks as regions
    n = len(ds[split_dim])
    splits = list(range(0,n,split_interval))

    # Make sure the last item in the list covers the full length of the time on our dataset
    if splits[-1] != n:
        splits = splits + [n]
    
    for ii in tqdm(range(len(splits)-1)):
        print(f'Writing split {ii}')
        # TODO: put some retry logic in here...
        start = splits[ii]
        stop = splits[ii+1]
        
        ds_write = ds.isel({split_dim:slice(start, stop)})
        print(f'Start: {ds_write[split_dim][0].data}')
        print(f'Stop: {ds_write[split_dim][-1].data}')
        
        # strip everything except the values
        drop_vars = non_split_vars_coords+other_dims
        ds_write = ds_write.drop_vars(drop_vars)
        
        ds_write.to_zarr(store, region={split_dim:slice(start, stop)}, mode='a')#why are the variables not instantiated in the init step
    print('Consolidating metadata')
    zarr.consolidate_metadata(store)
    print('DONE 🎉')