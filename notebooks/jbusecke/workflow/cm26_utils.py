import xarray as xr
import numpy as np
import gcsfs
import xesmf as xe

raise DeprecationError('Use the main module instead')

# def load_and_combine_cm26(filesystem: gcsfs.GCSFileSystem, inline_array=False)-> xr.Dataset:
#     """Loads, combines, and preprocesses CM2.6 data
#     Steps:
#     - Interpolate ocean velocities on ocean tracer points (with xgcm)
#     - Regrid atmospheric variables to ocean tracer grid (with xesmf)
#     - Match time and merge datasets
#     - Adjust units for aerobulk input
#     - Calculate relative wind components
#     """
#     kwargs = dict(consolidated=True, use_cftime=True, inline_array=inline_array, engine='zarr')#,
#     print('Load Data')
#     mapper = filesystem.get_mapper("gs://cmip6/GFDL_CM2_6/control/surface")
#     # ds_ocean = xr.open_dataset(mapper, chunks='auto', **kwargs)
#     # ds_ocean = xr.open_dataset(mapper, chunks={'time':2}, **kwargs)
#     ds_ocean = xr.open_dataset(mapper, chunks={'time':3}, **kwargs)
#     # cat = open_catalog("https://raw.githubusercontent.com/pangeo-data/pangeo-datastore/master/intake-catalogs/ocean/GFDL_CM2.6.yaml")
#     # ds_ocean  = cat["GFDL_CM2_6_control_ocean_surface"].to_dask()

#     # ds_flux  = cat["GFDL_CM2_6_control_ocean_boundary_flux"].to_dask()
#     mapper = filesystem.get_mapper("gs://cmip6/GFDL_CM2_6/control/ocean_boundary")
#     # ds_flux = xr.open_dataset(mapper, chunks='auto', **kwargs)
#     # ds_flux = xr.open_dataset(mapper, chunks={'time':2}, **kwargs)
#     ds_flux = xr.open_dataset(mapper, chunks={'time':3}, **kwargs)


#     # xarray says not to do this
#     # ds_atmos = xr.open_zarr('gs://cmip6/GFDL_CM2_6/control/atmos_daily.zarr', chunks={'time':1}, **kwargs)
#     mapper = filesystem.get_mapper("gs://cmip6/GFDL_CM2_6/control/atmos_daily.zarr")
#     # ds_atmos = xr.open_dataset(mapper, chunks={'time':120}, **kwargs)
#     # ds_atmos = xr.open_dataset(mapper, chunks={'time':2}, **kwargs)
#     # ds_atmos = xr.open_dataset(mapper, chunks={'time':3}, **kwargs)
#     ds_atmos = xr.open_dataset(mapper, chunks={'time':120}, **kwargs).chunk({'time':3})

#     # # instead do this
#     # ds_atmos = ds_atmos.chunk({'time':1})

#     mapper = filesystem.get_mapper("gs://cmip6/GFDL_CM2_6/grid")
#     ds_oc_grid = xr.open_dataset(mapper, chunks={}, **kwargs)
#     # ds_oc_grid  = cat["GFDL_CM2_6_grid"].to_dask()

#     print('Align in time')
#     # cut to same time
#     all_dims = set(list(ds_ocean.dims)+list(ds_atmos.dims))
#     ds_ocean, ds_atmos = xr.align(
#         ds_ocean,
#         ds_atmos,
#         join='inner',
#         exclude=(di for di in all_dims if di !='time')
#     )

#     print('Interpolating ocean velocities')
#     # interpolate ocean velocities onto the tracer points using xgcm
#     from xgcm import Grid
#     # add xgcm comodo attrs
#     ds_ocean['xu_ocean'].attrs['axis'] = 'X'
#     ds_ocean['xt_ocean'].attrs['axis'] = 'X'
#     ds_ocean['xu_ocean'].attrs['c_grid_axis_shift'] = 0.5
#     ds_ocean['xt_ocean'].attrs['c_grid_axis_shift'] = 0.0
#     ds_ocean['yu_ocean'].attrs['axis'] = 'Y'
#     ds_ocean['yt_ocean'].attrs['axis'] = 'Y'
#     ds_ocean['yu_ocean'].attrs['c_grid_axis_shift'] = 0.5
#     ds_ocean['yt_ocean'].attrs['c_grid_axis_shift'] = 0.0
#     grid = Grid(ds_ocean)
#     ds_ocean['u_ocean'] = grid.interp_like(ds_ocean['usurf'], ds_ocean['surface_temp'])
#     ds_ocean['v_ocean'] = grid.interp_like(ds_ocean['vsurf'], ds_ocean['surface_temp'])

#     print('Regrid Atmospheric Data')
#     # Start regridding the atmosphere onto the ocean grid
#     # Load precalculated regridder weights from group bucket
#     path = 'ocean-transport-group/scale-aware-air-sea/regridding_weights/CM26_atmos2ocean.zarr'
#     mapper = filesystem.get_mapper(path)
#     ds_regridder = xr.open_zarr(mapper).load()
#     regridder = xe.Regridder(
#         ds_atmos.olr.to_dataset(name='dummy').isel(time=0).reset_coords(drop=True),# this is the same dumb problem I keep having with
#         ds_ocean.surface_temp.to_dataset(name='dummy').isel(time=0).reset_coords(drop=True),
#         'bilinear',
#         weights=ds_regridder,
#         periodic=True
#     )
#     ds_atmos_regridded = regridder(ds_atmos[['slp', 'v_ref', 'u_ref', 't_ref', 'q_ref', 'wind']])# We are only doing noskin for now , 'swdn_sfc', 'lwdn_sfc'

#     ## combine into merged dataset
#     ds_merged = xr.merge(
#         [
#             ds_atmos_regridded,
#             ds_ocean[['surface_temp', 'u_ocean', 'v_ocean']],
#         ]
#     )
#     print('Modify units')
#     # ds_merged = ds_merged.transpose(
#     #     'xt_ocean', 'yt_ocean', 'time'
#     # )
#     # fix units for aerobulk
#     ds_merged['surface_temp'] = ds_merged['surface_temp'] + 273.15
#     ds_merged['slp'] = ds_merged['slp'] * 100 # check this

#     print('Mask nans')
#     # atmos missing values are filled with 0s, which causes issues with the filtering
#     # Ideally this should be masked before the regridding, but xesmf fills with 0 again...
#     mask = ~np.isnan(ds_merged['surface_temp'])
#     for mask_var in ['slp', 't_ref', 'q_ref']:
#         ds_merged[mask_var] = ds_merged[mask_var].where(mask)

#     # Calculate relative wind
#     print('Calculate relative wind')
#     ds_merged['u_relative'] = ds_merged['u_ref'] - ds_merged['u_ocean']
#     ds_merged['v_relative'] = ds_merged['v_ref'] - ds_merged['v_ocean']

#     return ds_merged
