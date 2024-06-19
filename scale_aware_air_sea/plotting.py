import numpy as np
import xarray as xr
from scipy import ndimage as nd


# from https://stackoverflow.com/questions/3662361/fill-in-missing-values-with-nearest-neighbour-in-python-numpy-masked-arrays
def fill(data: np.ndarray, invalid: np.ndarray):
    """
    Replace the value of invalid 'data' cells (indicated by 'invalid')
    by the value of the nearest valid data cell

    Input:
        data:    numpy array of any dimension
        invalid: a binary array of same shape as 'data'. True cells set where data
                 value should be replaced.
                 If None (default), use: invalid  = np.isnan(data)

    Output:
        Return a filled array.
    """
    # import numpy as np
    # import scipy.ndimage as nd
    ind = nd.distance_transform_edt(
        invalid, return_distances=False, return_indices=True
    )
    return data[tuple(ind)]


def fill_da(da: xr.DataArray) -> xr.DataArray:
    """fills nans in dataarray"""
    data = da.data
    filled_data = fill(data, np.isnan(data))
    da.data = filled_data
    return da


def centered_shrink_axes(ax, factor):
    bbox = ax.get_position()
    left = bbox.x0 + (bbox.width * factor / 2)
    bottom = bbox.y0 + (bbox.height * factor / 2)
    width = bbox.width * factor
    height = bbox.height * factor
    ax.set_position([left, bottom, width, height])
