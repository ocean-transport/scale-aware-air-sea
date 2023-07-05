import gcsfs
import fsspec
import numpy as np
import xarray as xr
import xesmf as xe


def load_and_combine_cesm(
    filesystem: gcsfs.GCSFileSystem, inline_array=False
) -> xr.Dataset:
    """Loads, combines, and preprocesses CM2.6 data
    Steps:
    - Interpolate ocean velocities on ocean tracer points (with xgcm)
    - Regrid atmospheric variables to ocean tracer grid (with xesmf)
    - Match time and merge datasets
    - Adjust units for aerobulk input
    - Calculate relative wind components
    """
    kwargs = dict(
        consolidated=True, use_cftime=True, inline_array=inline_array, engine="zarr"
    )  # ,
    print("Load Data")
    # Load ocean data
    mapper = filesystem.get_mapper("gs://pangeo-cesm-pop/control")
    ds_ocean = xr.open_dataset(mapper, chunks={"time": 1}, **kwargs)
    
    # Load atmospheric data
    mapper = fsspec.get_mapper("https://ncsa.osn.xsede.org/Pangeo/pangeo-forge-test/prod/recipe-run-502/pangeo-forge/cesm-atm-025deg-feedstock/cesm-atm-025deg.zarr")
    ds_atmos = xr.open_dataset(mapper, chunks={}, **kwargs)
    ds_atmos = ds_atmos.chunk({"time":1})

    print("Align in time")
    # cut to same time
    all_dims = set(list(ds_ocean.dims)+list(ds_atmos.dims))
    ds_ocean, ds_atmos = xr.align(
        ds_ocean,
        ds_atmos,
        join='inner',
        exclude=(di for di in all_dims if di !='time')
    )

    print("Interpolating ocean velocities")
    # interpolate ocean velocities onto the tracer points using xgcm
    from xgcm import Grid
    import pop_tools
    
    grid, ds_ocean = pop_tools.to_xgcm_grid_dataset(ds_ocean, periodic=False)
    
    # Fill missing ocean values with 0
    sst_wet_mask = ~np.isnan(ds_ocean['SST'].isel(time=0))
    
    # Do the interpolation
    ds_ocean["u_ocean"] = grid.interp_like(
        ds_ocean["U1_1"].fillna(0), ds_ocean["SST"]
    ).where(sst_wet_mask)
    ds_ocean["v_ocean"] = grid.interp_like(
        ds_ocean["V1_1"].fillna(0), ds_ocean["SST"]
    ).where(sst_wet_mask)

    print("Regrid Atmospheric Data")
    # Start regridding the atmosphere onto the ocean grid
    
    # CESM grid variables are in the datasets, so grab for single time step
    atmos_grid = ds_atmos.isel(time=0)
    ocean_grid = ds_ocean.isel(time=0).drop_vars([co for co in ds_ocean.coords if co not in ['TLONG','TLAT']])

    # Load precalculated regridder weights from group bucket
    path = 'gs://leap-persistent/jbusecke/scale-aware-air-sea/regridding_weights/ncar_atmos2ocean.zarr'
    mapper_regrid = filesystem.get_mapper(path)
    ds_regridder = xr.open_zarr(mapper_regrid).load()
    regridder = xe.Regridder(
        atmos_grid,
        ocean_grid,
        'bilinear',
        weights=ds_regridder,
        periodic=True
    )
    ds_atmos_regridded = regridder(ds_atmos[['TS', 'UBOT', 'VBOT', 'QREFHT', 'PSL','U10','TREFHT']])

    # Combine into merged dataset
    ds_merged = xr.merge(
        [
            ds_atmos_regridded.chunk({'time':1}), # to have more manageable sized chunks (same as ocean variables)
            ds_ocean[['SST','u_ocean','v_ocean']] #.chunk({'time':73}), # to have same chunksizes as atmospheric variables
        ]
    )
    
    print("Mask nans")
    # Atmos missing values are filled with single floats (not sure how these values are chosen)
    # Ideally this should be masked before the regridding, but xesmf fills with 0 again...
    mask = ~np.isnan(ds_ocean['SST'].isel(time=0).reset_coords(drop=True))
    # mask = ds_ocean['SST'].reset_coords(drop=True)>3
    # for mask_var in ['PSL', 'TREFHT', 'QREFHT', 'VBOT', 'UBOT','SST']:
    ds_merged = ds_merged.where(mask)
    
    # also apply this mask to certain coordinates from the grid dataset
    for mask_coord in ['TAREA']:
        # ds_merged.coords[mask_coord] = ds_merged[mask_coord].where(mask.isel(time=0).drop('time'),0.0).astype(np.float64)
        ds_merged.coords[mask_coord] = ds_merged[mask_coord].where(mask,0.0).astype(np.float64)
#     # The casting to float64 is needed to avoid that weird bug where the manual global weighted ave
#     # is not close to the xarray weighted mean (I was not able to reproduce this with an example)
    
#     # Ideally this should be masked before the regridding,
#     # but xesmf fills with 0 again...
#     mask = ~np.isnan(ds_merged["surface_temp"])
#     for mask_var in ["slp", "t_ref", "q_ref"]:
#         ds_merged[mask_var] = ds_merged[mask_var].where(mask)

    # Define ice mask and save for later use

    print("Modify units")
    # fix units for aerobulk
    ds_merged["SST"] = ds_merged["SST"] + 273.15 # convert from degC to K
    ds_merged["TAREA"] = ds_merged["TAREA"] / 10000 # convert from cm^2 to m^2
    ds_merged["u_ocean"] = 0.01*ds_merged["u_ocean"] # convert from cm/s to m/s
    ds_merged["v_ocean"] = 0.01*ds_merged["v_ocean"] # convert from cm/s to m/s

    # Calculate relative wind
    print("Calculate relative wind")
    ds_merged["u_relative"] = ds_merged["UBOT"] - ds_merged["u_ocean"]
    ds_merged["v_relative"] = ds_merged["VBOT"] - ds_merged["v_ocean"]

    return ds_merged
