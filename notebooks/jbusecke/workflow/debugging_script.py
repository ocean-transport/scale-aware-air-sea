import xarray as xr
from aerobulk import noskin

def noskin_ds_wrapper(ds_in):
    ds_out = xr.Dataset()
    ds_in = ds_in.copy(deep=False)
    
    sst = ds_in.surface_temp + 273.15
    t_zt = ds_in.t_ref
    hum_zt = ds_in.q_ref
    u_zu = ds_in.u_ref
    v_zu = ds_in.v_ref
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
        algo='ecmwf',
        zt=2,
        zu=10,
    )
    ds_out['ql'] = ql
    ds_out['qh'] = qh
    ds_out['evap'] = evap
    ds_out['taux'] = taux 
    ds_out['tauy'] = tauy
    return ds_out

# load the tempsave file from `cm26_pipeline.ipynb`
ds_coarsened = xr.open_dataset('test_coarsened_filled.nc')
ds_coarse_res = noskin_ds_wrapper(ds_coarsened)