def get_params(version:str, test:bool=True) -> dict[str, str]:
    bucket = 'gs://leap-persistent/jbusecke' # equivalent to os.environ['PERSISTENT_BUCKET'], but this should work for all collaborators
    scratch = 'gs://leap-scratch/jbusecke'
    suffix = 'test' if test else ''
    n_coarsen = 50
    version_full = version+suffix
    global_params = {
        'filter_type':"gaussian",
        'filter_scale':50,
        'n_coarsen': n_coarsen,
        'version': version_full,
        'paths':{
            model:{
                'scratch': f"{scratch}/scale-aware-air-sea/temp/{model}_{version_full}.zarr",
                'filter': f"{bucket}/scale-aware-air-sea/preprocessed/{model}_filter_{version_full}.zarr",
                'coarse': f"{bucket}/scale-aware-air-sea/preprocessed/{model}_coarse_{n_coarsen}_{version_full}.zarr",
                'filter_fluxes': f"{bucket}/scale-aware-air-sea/results/{model}_fluxes_filter_{version_full}.zarr", 
                'coarse_fluxes': f"{bucket}/scale-aware-air-sea/results/{model}_fluxes_coarse_{n_coarsen}_{version_full}.zarr",
                'filter_decomposition': f"{bucket}/scale-aware-air-sea/results/{model}_fluxes_filter_decomposed_{version_full}.zarr",
                'coarse_decomposition': f"{bucket}/scale-aware-air-sea/results/{model}_fluxes_coarse_decomposed_{n_coarsen}_{version_full}.zarr",
            } for model in ['CM26']
        }
    }
    return global_params