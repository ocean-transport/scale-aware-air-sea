from dask.diagnostics import ProgressBar
import xarray as xr
from aerobulk import skin, noskin
from tqdm.auto import tqdm
from intake import open_catalog
import gcsfs
import xesmf as xe

def write_split_zarr(store, ds, split_dim='time', chunks=1, split_interval=180):
    """Write a large dataset in a sequence of batches split along one of the dataset dimensions.
    This can be helpful to e.g. avoid problems with overly eager dask schedulers
    """
    # Got my inspiration for this mostly here: https://github.com/pydata/xarray/issues/6069
    
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
        
        with ProgressBar():
            ds_write.to_zarr(store, region={split_dim:slice(start, stop)}, mode='a')#why are the variables not instantiated in the init step
            
# TODO: This is model agnostic and should live somewhere else?         
def noskin_ds_wrapper(ds_in, algo='ecmwf', **kwargs):
    ds_out = xr.Dataset()
    ds_in = ds_in.copy(deep=False)
    
    sst = ds_in.surface_temp + 273.15
    t_zt = ds_in.t_ref
    hum_zt = ds_in.q_ref
    u_zu = ds_in.u_relative
    v_zu = ds_in.v_relative
    slp = ds_in.slp * 100 # check this
    zu = 10
    zt = 2
    
    ql, qh, taux, tauy, evap =  noskin(
        sst,
        t_zt,
        hum_zt,
        u_zu,
        v_zu,
        slp=slp,
        algo=algo,
        zt=zt,
        zu=zu,
        **kwargs
    )
    ds_out['ql'] = ql
    ds_out['qh'] = qh
    ds_out['evap'] = evap
    ds_out['taux'] = taux 
    ds_out['tauy'] = tauy
    return ds_out

def load_and_merge_cm26(regridder_token):
    kwargs = dict(consolidated=True, use_cftime=True)
    cat = open_catalog("https://raw.githubusercontent.com/pangeo-data/pangeo-datastore/master/intake-catalogs/ocean/GFDL_CM2.6.yaml")
    ds_ocean  = cat["GFDL_CM2_6_control_ocean_surface"].to_dask()
    ds_flux  = cat["GFDL_CM2_6_control_ocean_boundary_flux"].to_dask()
    # xarray says not to do this
    # ds_atmos = xr.open_zarr('gs://cmip6/GFDL_CM2_6/control/atmos_daily.zarr', chunks={'time':1}, **kwargs)
    ds_atmos = xr.open_zarr('gs://cmip6/GFDL_CM2_6/control/atmos_daily.zarr', **kwargs)
    ds_oc_grid  = cat["GFDL_CM2_6_grid"].to_dask()
    # cut to same time
    all_dims = set(list(ds_ocean.dims)+list(ds_atmos.dims))
    ds_ocean, ds_atmos = xr.align(
        ds_ocean,
        ds_atmos,
        join='inner',
        exclude=(di for di in all_dims if di !='time')
    )
    # instead do this
    ds_atmos = ds_atmos.chunk({'time':1})
    
    fs = gcsfs.GCSFileSystem(token=regridder_token)
    path = 'ocean-transport-group/scale-aware-air-sea/regridding_weights/CM26_atmos2ocean.zarr'
    mapper = fs.get_mapper(path)
    ds_regridder = xr.open_zarr(mapper).load()
    regridder = xe.Regridder(
        ds_atmos.olr.to_dataset(name='dummy').isel(time=0),
        ds_ocean.surface_temp.to_dataset(name='dummy').isel(time=0),
        'bilinear',
        weights=ds_regridder,
        periodic=True
    )
    ds_atmos_regridded = regridder(ds_atmos[['slp', 'v_ref', 'u_ref', 't_ref', 'q_ref', 'wind']])# We are only doing noskin for now , 'swdn_sfc', 'lwdn_sfc'

    ## combine into merged dataset
    ds_merged = xr.merge(
        [
            ds_atmos_regridded,
            ds_ocean[['surface_temp']],
        ]
    )
    ds_merged = ds_merged.transpose(
        'xt_ocean', 'yt_ocean', 'time'
    )
    return ds_merged