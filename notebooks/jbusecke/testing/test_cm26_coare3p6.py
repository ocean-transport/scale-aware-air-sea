import time
import numpy as np
import xarray as xr
from aerobulk import flux_noskin
import itertools
import matplotlib.pyplot as plt

## manually recreate the wrapper. This wont be necessary after the PR is merged
def flux_noskin_xr(
    sst, t_zt, hum_zt, u_zu, v_zu, slp=101000.0, algo="coare3p0", zt=10, zu=2, niter=1
):
    # TODO do we need to make the "time" dimension special?

    sst, t_zt, hum_zt, u_zu, v_zu, slp = xr.broadcast(
        sst, t_zt, hum_zt, u_zu, v_zu, slp
    )

    if len(sst.dims) < 3:
        # TODO promote using expand_dims?
        raise NotImplementedError
    if len(sst.dims) > 4:
        # TODO iterate over extra dims? Or reshape?
        raise NotImplementedError

    out_vars = xr.apply_ufunc(
        flux_noskin,
        sst,
        t_zt,
        hum_zt,
        u_zu,
        v_zu,
        slp,
        input_core_dims=[()] * 6,
        output_core_dims=[()] * 5,
        # input_core_dims=[("dim_0", "dim_1", "dim_2")] * 6,
        # output_core_dims=[("dim_0", "dim_1", "dim_2")] * 5,
        dask="parallelized",
        kwargs=dict(
            algo=algo,
            zt=zt,
            zu=zu,
            niter=niter,
        ),
        output_dtypes=[sst.dtype]
        * 5,  # deactivates the 1 element check which aerobulk does not like
    )

    if not isinstance(out_vars, tuple) or len(out_vars) != 5:
        raise TypeError("F2Py returned unexpected types")

    if any(var.ndim != 3 for var in out_vars):
        raise ValueError(
            f"f2py returned result of unexpected shape. Got {[var.shape for var in out_vars]}"
        )

    # TODO if dimensions promoted squeeze them out before returning

    return out_vars  # currently returns only 3D arrays

# Load CM2.6 data
import fsspec
import xesmf as xe
import os
from intake import open_catalog
kwargs = dict(consolidated=True, use_cftime=True)
cat = open_catalog("https://raw.githubusercontent.com/pangeo-data/pangeo-datastore/master/intake-catalogs/ocean/GFDL_CM2.6.yaml")
ds_ocean  = cat["GFDL_CM2_6_control_ocean_surface"].to_dask()
ds_flux  = cat["GFDL_CM2_6_control_ocean_boundary_flux"].to_dask()
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

# for testing, rechunk everything to single time steps
ds_ocean = ds_ocean.chunk({'time':1})
ds_atmos = ds_atmos.chunk({'time':1})
ds_flux = ds_flux.chunk({'time':1})

# Load regridding weights and regrid all fields onto ocean grid
PANGEO_SCRATCH = os.environ['PANGEO_SCRATCH']
# From `upload_cm26_weights_from_gyre.ipynb`
mapper = fsspec.get_mapper(f'{PANGEO_SCRATCH}/jbusecke/test_cm26_xesmf_weights_cloud.zarr')
ds_regridder = xr.open_zarr(mapper)#.persist()

regridder = xe.Regridder(
    ds_atmos.olr.isel(time=0),
    ds_ocean.surface_temp.isel(time=0),
    'bilinear',
    weights=ds_regridder,
    periodic=True
)
regridder

ds_atmos_regridded = regridder(ds_atmos[['slp', 'v_ref', 'u_ref', 't_ref', 'q_ref', 'swdn_sfc', 'lwdn_sfc']])
ds_atmos_regridded

## combine into merged dataset
ds_merged = xr.merge(
    [
        ds_atmos_regridded,
        ds_ocean[['surface_temp']],
    ]
)
ds_merged

# TODO: run this with dask to get global fields...Currently blows out the memory of even the largest server
from dask.diagnostics import ProgressBar
with ProgressBar():
    ds_test = ds_merged.isel(
        time=np.arange(0, 180, 60),
        xt_ocean=slice(500, 1800),
        yt_ocean=slice(800, 2000)
    ).transpose(
        'xt_ocean', 'yt_ocean', 'time'
    ).load()

### TEST what error I get for coare3.6
out = flux_noskin_xr(
    ds_test['surface_temp']+ 273.15,
    ds_test['t_ref'],
    ds_test['q_ref'],
    ds_test['u_ref'],
    ds_test['v_ref'],
    slp = ds_test['slp']* 100,
    niter=1,
    algo='coare3p6'
)
