## Production (Debug) Script for CM2.6 model

from multiprocessing.pool import ThreadPool
import dask
dask.config.set(pool=ThreadPool(8))# this worked for ecmwf and ncar

from aerobulk import skin, noskin
import fsspec
import xarray as xr
import numpy as np
import xesmf as xe
import os
from intake import open_catalog
import matplotlib.pyplot as plt

import json
import gcsfs
from dask.diagnostics import ProgressBar
from cm26_utils import write_split_zarr, noskin_ds_wrapper

# 👇 replace with your key
with open('/home/jovyan/keys/pangeo-forge-ocean-transport-4967-347e2048c5a1.json') as token_file:
    token = json.load(token_file)
fs = gcsfs.GCSFileSystem(token=token)
subfolder_full = 'ocean-transport-group/scale-aware-air-sea/outputs/temp/'
subfolder_final = 'ocean-transport-group/scale-aware-air-sea/outputs/'

# for testing
appendix='_test'
# appendix = ''

# algo = 'coare3p6'
# algo='ncar'
algo='ecmwf'
# algo='andreas'


#######################################################
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


########################################################
fs = gcsfs.GCSFileSystem(token=token)
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
ds_atmos_regridded

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
print("TEST CUT DATASET")
ds_merged = ds_merged.isel(time=slice(100,140))

############################# wind cutting ##############
# ok so lets not add nans into fields like above. Instead, lets see in which timesteps this actually occurs and for noe completely ignore these timesteps
# This is not ideal in the long run, but maybe at least gives us a way to output

# ds_cut = ds_merged.isel(time=slice(0,500))
# wind = ds_cut.wind

wind = ds_merged.wind
threshold = 30
with ProgressBar():
    strong_wind_cells = (wind > threshold).sum(['xt_ocean','yt_ocean']).load()

strong_wind_index = strong_wind_cells > 0

# double check that these events are still rare in space and time
n_ocean_cells = xr.ones_like(ds_merged.slp.isel(time=0)).sum().load() * 0.7 # assuming that ~70% of total cells are ocean

(strong_wind_cells/n_ocean_cells*100).plot()
plt.ylabel(f'ocean cells with wind > {threshold} m [%]')

idx = np.where(~strong_wind_index)[0]
ds_ds_merged = ds_merged.isel(time=idx)
ds_ds_merged

# ds_out = noskin_ds_wrapper(ds_merged, algo=algo, input_range_check=False)
ds_out = noskin_ds_wrapper(ds_merged, algo=algo, input_range_check=True)

# Ad-hoc hack (splitting the dataset into batches and append to zarr store
path = f'{subfolder_full}CM26_high_res_output_{algo}{appendix}.zarr'
mapper = fs.get_mapper(path)
print(f"Writing to {path}")

overwrite = True
if fs.exists(path) and overwrite:
# # # delete the mapper (only uncomment if you want to start from scratch!)
    print("DELETE existing store")
    fs.rm(path, recursive=True)

write_split_zarr(mapper, ds_out, split_interval=64)
