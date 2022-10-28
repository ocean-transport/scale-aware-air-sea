#!/usr/bin/env python
# coding: utf-8

# # Debug issues with exceeding wind stress
# 
# ## Tasks
# - Find timestep(s) that exhibit the behavior
# - Confirm that this behavior only affects certain algorithms
# - Check if masking the windspeed helps as an intermediate step

# In[1]:


# !mamba install aerobulk-python -y


# In[2]:


from multiprocessing.pool import ThreadPool
import dask
dask.config.set(pool=ThreadPool(4))# Seems to be a safe bet for all algos on the huge machine


# In[3]:


import json
import xarray as xr
import matplotlib.pyplot as plt
from dask.diagnostics import ProgressBar
from cm26_utils import write_split_zarr, noskin_ds_wrapper, load_and_merge_cm26

# 👇 replace with your key 
with open('/home/jovyan/keys/pangeo-forge-ocean-transport-4967-347e2048c5a1.json') as token_file:
    token = json.load(token_file)


# In[4]:


ds_merged = load_and_merge_cm26(token)


# ## Thoughts
# - I think the input_range_check is useless here since values well under the range of |50 m/s| seem to upset the other algos

# ## Testing

# In[5]:


ds_merged = ds_merged.isel(time=slice(90, 130))


# This datasets is enough to produce failures in all algos except ncar + ecmwf

# In[6]:


# can we reproduce the failure with this simple example?
# YES if converted to a python script first (kind of annoying)
# I did `jupyter nbconvert --to python cm26_pipeline-debug.ipynb`
# and then I get the error about the wind stress.

# I have executed this with all algos and I get crashes for: 
# 'coare3p6'
# 'andreas'
# 'coare3p0'

# algo='andreas'
# algo='ecmwf'
algo='ncar'
# algo = 'coare3p0'
# algo = 'coare3p6'

ds_out = noskin_ds_wrapper(ds_merged, algo=algo, input_range_check=False)
ds_out

# These work without issues
# 'ncar'
# 'ecmwf'


with ProgressBar():
    ds_out.load()


# ## Investigate the max windstress values we are getting with the working algos
# 
# Is there a correlation betweewn max wind speeds and stresses? Yeah definitely!

# In[ ]:


working_algos = ['ncar','ecmwf'] #,'andreas', 'andreas' can we reproduce the failure with this simple example?
datasets = []
for algo in working_algos:
    ds_out = noskin_ds_wrapper(ds_merged, algo=algo, input_range_check=False)
    with ProgressBar():
        ds_out.load()
    # calculate the stress magnitude
    stress = (ds_out.taux **2 + ds_out.tauy**2)**0.5
    stress_max = stress.max(['xt_ocean', 'yt_ocean']).assign_coords(algo=algo)
    print(stress_max)
    datasets.append(stress_max)


# In[ ]:


(ds_merged.wind.max(['xt_ocean', 'yt_ocean'])/10).plot(label='max wind speed scaled')
plt.legend()
max_stresses = xr.concat(datasets, dim='algo')
max_stresses.plot(hue='algo')
plt.grid()


# ## Ok can we actually get around this and get some results at all?
# If not we need to raise the tau cut of in aerobulk.
# 
# My simple approach right here is to set every wind value larger than `threshold` to zero. This is not a feasible solution for our processing, but I just want to see how low we have to go to get all algos to go through! 

# In[ ]:


failing_algos = ['andreas', 'coare3p0','coare3p6']

threshold = 3
datasets = []
for algo in failing_algos:
    ds_masked = ds_merged.copy(deep=False)
    mask = ds_masked.wind>threshold
    ds_masked['u_ref'] = ds_masked['u_ref'].where(mask, 0)
    ds_masked['v_ref'] = ds_masked['v_ref'].where(mask, 0)
    
    break
    ds_out = noskin_ds_wrapper(ds_merged, algo=algo, input_range_check=False)
    with ProgressBar():
        ds_out.load()
    # calculate the stress magnitude
    stress = (ds_out.taux **2 + ds_out.tauy**2)**0.5
    stress_max = stress.max(['xt_ocean', 'yt_ocean']).assign_coords(algo=algo)
    print(stress_max)
    datasets.append(stress_max)

