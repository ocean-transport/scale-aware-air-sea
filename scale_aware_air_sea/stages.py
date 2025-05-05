from xgcm import Grid
import pop_tools
import gcsfs
import fsspec
import numpy as np
import xesmf as xe
import xarray as xr


def load_cesm_data(fs: gcsfs.GCSFileSystem) -> tuple[xr.Dataset, xr.Dataset]:
    kwargs = dict(consolidated=True, use_cftime=True, engine="zarr")

    # Load ocean data
    ocean_path = "gs://pangeo-cesm-pop/control"
    ds_ocean = xr.open_dataset(fs.get_mapper(ocean_path), chunks={"time": 1}, **kwargs)

    # Load atmospheric data
    atmos_path = "https://ncsa.osn.xsede.org/Pangeo/pangeo-forge-test/prod/recipe-run-502/pangeo-forge/cesm-atm-025deg-feedstock/cesm-atm-025deg.zarr"
    ds_atmos = xr.open_dataset(fsspec.get_mapper(atmos_path), chunks={}, **kwargs)
    ds_atmos = ds_atmos.chunk({"time": 1})

    print("Interpolating ocean velocities")
    # interpolate ocean velocities onto the tracer points using xgcm
    grid, ds_ocean = pop_tools.to_xgcm_grid_dataset(ds_ocean, periodic=True)

    # Fill missing ocean values with 0
    sst_wet_mask = ~np.isnan(ds_ocean["SST"].isel(time=0))

    # Do the interpolation
    ds_ocean["u_ocean"] = grid.interp_like(
        ds_ocean["U1_1"].fillna(0), ds_ocean["SST"]
    ).where(sst_wet_mask)
    ds_ocean["v_ocean"] = grid.interp_like(
        ds_ocean["V1_1"].fillna(0), ds_ocean["SST"]
    ).where(sst_wet_mask)

    # rename this dataset to CM2.6 conventions (purely subjective choice here)
    rename_dict = {
        "nlon_t": "xt_ocean",
        "nlat_t": "yt_ocean",
        "TLAT": "geolat_t",
        "TLONG": "geolon_t",
        "UBOT": "u_ref",
        "VBOT": "v_ref",
        "SST": "surface_temp",
        "TREFHT": "t_ref",
        "QREFHT": "q_ref",
        "PSL": "slp",
        "TAREA": "area_t",
    }
    ds_ocean = ds_ocean.rename(
        {k: v for k, v in rename_dict.items() if k in ds_ocean.variables}
    )
    ds_atmos = ds_atmos.rename(
        {k: v for k, v in rename_dict.items() if k in ds_atmos.variables}
    )

    # fix units for aerobulk (TODO: Maybe this could be handled better with pint-xarray?
    print("Modify units")
    ds_ocean["surface_temp"] = ds_ocean["surface_temp"] + 273.15
    ds_ocean["u_ocean"] = 0.01 * ds_ocean["u_ocean"]  # convert from cm/s to m/s
    ds_ocean["v_ocean"] = 0.01 * ds_ocean["v_ocean"]  # convert from cm/s to m/s
    ds_ocean.coords["area_t"] = ds_ocean["area_t"] / 10000  # convert from cm^2 to m^2

    return ds_ocean, ds_atmos


def load_cm26_data(fs: gcsfs.GCSFileSystem) -> tuple[xr.Dataset, xr.Dataset]:
    kwargs = dict(consolidated=True, use_cftime=True, engine="zarr")

    print("Load Data")
    ocean_path = "gs://cmip6/GFDL_CM2_6/control/surface"
    ds_ocean = xr.open_dataset(fs.get_mapper(ocean_path), chunks={"time": 3}, **kwargs)

    grid_path = "gs://cmip6/GFDL_CM2_6/grid"
    ds_ocean_grid = xr.open_dataset(fs.get_mapper(grid_path), chunks={}, **kwargs)

    # combine all dataset on the ocean grid together
    ds_ocean = xr.merge([ds_ocean_grid, ds_ocean], compat="override")

    # xarray says not to do this
    # ds_atmos = xr.open_zarr('gs://cmip6/GFDL_CM2_6/control/atmos_daily.zarr', chunks={'time':1}, **kwargs) # noqa: E501
    atmos_path = "gs://cmip6/GFDL_CM2_6/control/atmos_daily.zarr"
    ds_atmos = xr.open_dataset(
        fs.get_mapper(atmos_path), chunks={"time": 120}, **kwargs
    ).chunk({"time": 3})

    print("Interpolating ocean velocities")
    # interpolate ocean velocities onto the tracer points using xgcm

    # add xgcm comodo attrs
    ds_ocean["xu_ocean"].attrs["axis"] = "X"
    ds_ocean["xt_ocean"].attrs["axis"] = "X"
    ds_ocean["xu_ocean"].attrs["c_grid_axis_shift"] = 0.5
    ds_ocean["xt_ocean"].attrs["c_grid_axis_shift"] = 0.0
    ds_ocean["yu_ocean"].attrs["axis"] = "Y"
    ds_ocean["yt_ocean"].attrs["axis"] = "Y"
    ds_ocean["yu_ocean"].attrs["c_grid_axis_shift"] = 0.5
    ds_ocean["yt_ocean"].attrs["c_grid_axis_shift"] = 0.0
    grid = Grid(ds_ocean)

    # fill missing values with 0, then interpolate.
    tracer_ref = ds_ocean["surface_temp"]
    sst_wet_mask = ~np.isnan(tracer_ref)

    ds_ocean["u_ocean"] = grid.interp_like(
        ds_ocean["usurf"].fillna(0), tracer_ref
    ).where(sst_wet_mask)
    ds_ocean["v_ocean"] = grid.interp_like(
        ds_ocean["vsurf"].fillna(0), tracer_ref
    ).where(sst_wet_mask)

    # rename the atmos data coordinates only to CESM conventions
    ds_atmos = ds_atmos.rename({"grid_xt": "lon", "grid_yt": "lat"})

    # fix units for aerobulk (TODO: Maybe this could be handled better with pint-xarray?
    print("Modify units")
    ds_ocean["surface_temp"] = ds_ocean["surface_temp"] + 273.15
    ds_atmos["slp"] = ds_atmos["slp"] * 100  # TODO: Double check this

    return ds_ocean, ds_atmos


def regrid_atmos(ds_ocean: xr.Dataset, ds_atmos: xr.Dataset) -> xr.Dataset:
    # Create the regridder (I could save this out to a nc file and upload it as a zarr
    # (as in pipeline/old_code/generate_regridding_weights_cm26.ipynb) but since we are
    # writing the full preprocessed dataset out to scratch anyways, why bother?
    # NOTE: The regridder weight calculation needs 36+ GB of ram available. Which might be important for later beam stages.

    # create stripped down versions of the input datasets, to make sure we are using these coordinates for regridding.
    ocean_sample = ds_ocean.drop(
        [co for co in ds_ocean.coords if co not in ["geolon_t", "geolat_t"]]
    )
    atmos_sample = ds_atmos.drop(
        [co for co in ds_atmos.coords if co not in ["lon", "lat"]]
    )

    regridder = regridder = xe.Regridder(
        atmos_sample,
        ocean_sample,
        "bilinear",
        periodic=True,
        unmapped_to_nan=True,  # I think i need to cut out the lon/lat values before this! Might save some .where()'s later
    )
    return regridder(ds_atmos)


def construct_ice_mask(ds: xr.Dataset) -> xr.DataArray:
    # Define Ice mask (TODO: raise issue discussing this)
    # I am choosing to use the same temp criterion here for both models.
    # We should add an appendix looking at the global difference between
    # using a time resolved vs maximally excluding ice_mask (e.g. max extent over a year)
    # I prototyped another method using the melt rate.
    # But the computation is super gnarly (see pipeline/step_00_ice_mask_brute_force_cm2.6.ipynb).
    return ds.surface_temp > 273.15


def preprocess(
    fs: gcsfs.GCSFileSystem, model: str, include_fluxes: bool = False
) -> xr.Dataset:
    # loading data
    print(f"{model}: Loading Data")
    if model == "CM26":
        load_func = load_cm26_data
    elif model == "CESM":
        load_func = load_cesm_data

    ds_ocean, ds_atmos = load_func(fs)

    print(f"{model}: Align in time")
    # cut to same time
    all_dims = set(list(ds_ocean.dims) + list(ds_atmos.dims))
    ds_ocean, ds_atmos = xr.align(
        ds_ocean,
        ds_atmos,
        join="inner",
        exclude=(di for di in all_dims if di != "time"),
    )

    ds_ocean.coords["ice_mask"] = construct_ice_mask(ds_ocean)

    # regrid atmospheric data
    print(
        f"{model}: Regridding atmosphere (this takes a while, because we are computing the weights on the fly)"
    )
    # TODO: maybe get some non-gapped lon/lats and only put those out after regridding?

    # make sure that the ocean lon/lat values have nans in the same locations as the ocean tracer fields
    tracer_ref_mask = ~np.isnan(ds_ocean["surface_temp"].isel(time=0, drop=True))
    lon_masked = ds_ocean.geolon_t.where(tracer_ref_mask)
    lat_masked = ds_ocean.geolat_t.where(tracer_ref_mask)
    area_masked = ds_ocean.area_t.where(tracer_ref_mask, 0.0)
    # # WHy the fuck does this not work?
    # ds_ocean_masked = ds_ocean.assign_coords({'geolat_t':lat_masked, 'geolon_t':lon_masked})
    # this works, but seriously wtf? The above also works in a cell below, just not in this function?
    ds_ocean.coords["geolon_t"].data = lon_masked.data
    ds_ocean.coords["geolat_t"].data = lat_masked.data
    ds_ocean.coords["area_t"].data = area_masked.data

    ds_atmos_regridded = regrid_atmos(ds_ocean, ds_atmos)

    # merge data on the ocean grid (and discard variables not needed for analysis)
    print(f"{model}: Merging on ocean tracer grid")
    atmos_vars = ["slp", "v_ref", "u_ref", "t_ref", "q_ref"]
    ocean_vars = ["surface_temp", "u_ocean", "v_ocean"]
    if include_fluxes:
        if model == "CESM":
            atmos_vars = atmos_vars + ["LHFLX", "SHFLX"]
        else:
            raise

    merge_datasets = [ds_ocean[ocean_vars], ds_atmos_regridded[atmos_vars]]
    ds_combined = xr.merge(merge_datasets)

    # Calculate relative wind
    print(f"{model}: Calculate relative wind")
    ds_combined["u_relative"] = ds_combined["u_ref"] - ds_combined["u_ocean"]
    ds_combined["v_relative"] = ds_combined["v_ref"] - ds_combined["v_ocean"]

    # Drop coordinates
    print(f"{model}: Drop extra coords")
    keep_coords = ["time", "geolon_t", "geolat_t", "area_t", "ice_mask"]
    ds_combined = ds_combined.drop(
        [co for co in ds_combined.coords if co not in keep_coords]
    )
    return ds_combined
