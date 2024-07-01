import xarray as xr
import intake
import xesmf as xe
from dask.diagnostics import ProgressBar


# load the ocean data
from intake import open_catalog
kwargs = dict(consolidated=True, use_cftime=True)
cat = open_catalog("https://raw.githubusercontent.com/pangeo-data/pangeo-datastore/master/intake-catalogs/ocean/GFDL_CM2.6.yaml")
ds  = cat["GFDL_CM2_6_control_ocean_surface"].to_dask()
ds_atmos = xr.open_zarr('gs://cmip6/GFDL_CM2_6/control/atmos_daily.zarr', **kwargs)

regridder = xe.Regridder(ds_atmos.olr.isel(time=0), ds.surface_temp.isel(time=0), 'bilinear', periodic=True) # all the atmos data is on the cell center AFAIK
