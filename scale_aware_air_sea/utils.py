import gcm_filters
import numpy as np
from typing import Mapping, Any
import xarray as xr
import zarr
from tqdm.auto import tqdm


def open_zarr(mapper, chunks={}):
    return xr.open_dataset(
        mapper, 
        engine='zarr',
        chunks=chunks,
        consolidated=True,
        inline_array=True
    )


def smooth_inputs(
    da: xr.DataArray,
    wet_mask: xr.DataArray,
    dims: list,
    filter_scale: int,
    filter_type: str = "taper",
) -> xr.DataArray:
    """Smoothes input using gcm-filters"""
    if filter_type == "gaussian":
        input_filter = gcm_filters.Filter(
            filter_scale=filter_scale,
            dx_min=1,
            filter_shape=gcm_filters.FilterShape.GAUSSIAN,
            grid_type=gcm_filters.GridType.REGULAR_WITH_LAND,
            grid_vars={"wet_mask": wet_mask},
        )
    elif filter_type == "taper":
        # transition_width = np.pi/2
        transition_width = np.pi
        input_filter = gcm_filters.Filter(
            filter_scale=filter_scale,
            transition_width=transition_width,
            dx_min=1,
            filter_shape=gcm_filters.FilterShape.TAPER,
            grid_type=gcm_filters.GridType.REGULAR_WITH_LAND,
            grid_vars={"wet_mask": wet_mask},
        )

    else:
        raise ValueError(
            f"`filter_type` needs to be `gaussian` or `taper', got {filter_type}"
        )
    out = input_filter.apply(da, dims=dims)
    out.attrs["filter_scale"] = filter_scale
    out.attrs["filter_type"] = filter_type
    return out


def smooth_inputs_dataset(
    ds: xr.Dataset,
    dims: list,
    filter_scale: int,
    timedim: str = "time",
    filter_type: str = "taper",
) -> xr.Dataset:
    """Wrapper that filters a whole dataset, generating a wet_mask from
    the nanmask of the first timestep (if time is present)."""
    ds_out = xr.Dataset()

    # create a wet mask that only allows values which are 'wet' in all variables

    wet_masks = []
    for var in ds.data_vars:
        da = ds[var]
        if timedim in da.dims:
            mask_da = da.isel({timedim: 0})
        else:
            mask_da = da
        wet_masks.append((~np.isnan(mask_da)))

    combined_wet_mask = xr.concat(wet_masks, dim="var").all("var").astype(int)

    for var in ds.data_vars:
        ds_out[var] = smooth_inputs(
            ds[var], combined_wet_mask, dims, filter_scale, filter_type=filter_type
        )

    ds_out.attrs["filter_scale"] = filter_scale
    ds_out.attrs["filter_type"] = filter_type
    return ds_out


def scale_separation(ds, filter_scale, mask):
    ds_filtered = smooth_inputs_dataset(ds, ["yt_ocean", "xt_ocean"], filter_scale)

    all_smoothing_options_except_full = [
        s for s in ds.smoothing.data if "full" not in s
    ]

    diff_filtered = ds_filtered.sel(smoothing="smooth_full") - ds_filtered.sel(
        smoothing=all_smoothing_options_except_full
    )
    diff_unfiltered = ds_filtered.sel(smoothing="smooth_full") - ds.sel(
        smoothing=all_smoothing_options_except_full
    )

    # assigne scale datasets
    ds_full = ds_filtered.sel(smoothing="smooth_full")

    ds_large_scale = ds.sel(smoothing="smooth_all")

    ds_small_scale = xr.concat(
        [
            diff_unfiltered.sel(smoothing="smooth_all"),  # the main result,
            diff_filtered.sel(
                smoothing=[s for s in diff_filtered.smoothing.data if "all" not in s]
            ),  # The mechanism 'hints'
        ],
        dim="smoothing",
    )

    # mask the outputs
    ds_full = ds_full.where(mask)
    ds_large_scale = ds_large_scale.where(mask)
    ds_small_scale = ds_small_scale.where(mask)

    return ds_full, ds_large_scale, ds_small_scale


def to_zarr_split(ds, mapper, split_dim="time", split_interval=1):
    print(f"Writing to {mapper.root} ...")

    n = len(ds[split_dim])
    splits = list(range(0, n, split_interval))

    # Make sure the last item in the list covers the full length
    # of the time on our dataset
    if splits[-1] != n:
        splits = splits + [n]

    split_datasets = []
    for ii in range(len(splits) - 1):
        start = splits[ii]
        stop = splits[ii + 1]
        split_datasets.append(ds.isel({split_dim: slice(start, stop)}))

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
    
def weighted_coarsen(ds:xr.Dataset, dim: Mapping[Any, int],  weight_coord:str, timedim='time', **kwargs) -> xr.Dataset:
    
    # Check that the weights have no missing values
    weights = ds[weight_coord]
    if np.isnan(weights).sum()>0:
        raise ValueError(f'Found missing values in weights coordinate ({weight_coord}). Please fill with zeros before.')
        
    # Make sure that the weights are matching the missing values in the input data 
    # (otherwise creation of aggregated area will be ambigous and depend on each variable)
    # the important thing to check is if a) all variables have the same mask and
    variable_missing = np.isnan(ds.to_array())
    
    if timedim in ds.dims:
        variable_missing = variable_missing.isel({timedim:0})
    
    variable_mask = variable_missing.any('variable').load() # loading because we need it multiple times
    variable_test = variable_missing.all('variable')
    if not variable_mask.equals(variable_test):
        raise ValueError('Found variables with non-matching missing values. ',
                         'Make sure that the missing values in **all** variables are in the same position.')
    
    # and b) if the weights have nonzero values that do not match the variables (this would lead to additional area being counted below) 
    weights_test = weights<=0
    if not np.allclose(variable_mask,weights_test):
        raise ValueError(
            'Missing values in variables are not matching locations of <=0 values in weights array. ',
            'Please change your weights to only have missing values or zeros where variables have missing values.'
        )
    
    # start the actual calculation
    ds_coarse = ds.coarsen(**dim, **kwargs)
    # construct internal/external dims
    construct_kwargs = {di:(di+'_external', di+'_internal') for di in dim}
    ds_construct = ds_coarse.construct(**construct_kwargs)
    
    # apply weighted mean over internal dimensions
    weights_coarse = ds_construct[weight_coord]
    aggregate_dims = [di+'_internal' for di in dim]
    ds_out = ds_construct.weighted(weights_coarse).mean(aggregate_dims)
    
    # add new area that corresponds to the area that was used for each coarse cell
    ds_out = ds_out.assign_coords(**{weight_coord:weights_coarse.sum(aggregate_dims)})
    
    # add other coordinates back
    coords_to_treat = [co for co in ds.coords if co != weight_coord and co not in ds_out.coords]
    treated_coords = {co:ds[co].coarsen({k:v for k,v in dim.items() if k in ds[co].dims}).mean() for co in coords_to_treat}
    ds_out = ds_out.assign_coords(**treated_coords)
    
    # rename to original names and return
    return ds_out.rename({di+'_external': di for di in dim})
