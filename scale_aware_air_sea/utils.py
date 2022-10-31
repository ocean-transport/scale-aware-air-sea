from tqdm.auto import tqdm
import xarray as xr
import zarr

def to_zarr_split(ds, mapper, split_dim='time', split_interval=1):
    #
#     if mapper.fs.exists(mapper.root):
#         raise ValueError(f'{mapper.root} already exists. Please delete manually before writing')
    
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