def get_params(version:str, test:bool=True) -> dict[str, str]:
    bucket = 'gs://leap-persistent/jbusecke' # equivalent to os.environ['PERSISTENT_BUCKET'], but this should work for all collaborators
    scratch = 'gs://leap-scratch/jbusecke'
    suffix = 'test' if test else ''
    n_coarsen = 50
    project_path = f"scale-aware-air-sea"
    version_full = version+suffix
    global_params = {
        'filter_type':"gaussian",
        'filter_scale':50,
        'n_coarsen': n_coarsen,
        'version': version_full,
        'paths':{
            model:{
                'ice_mask' : f"{bucket}/{project_path}/{version_full}/preprocessed/{model}_ice_mask.zarr",
                'scratch': f"{scratch}/{project_path}/{version_full}/temp/{model}.zarr",
                'filter': f"{bucket}/{project_path}/{version_full}/preprocessed/{model}_filter.zarr",
                'coarse': f"{bucket}/{project_path}/{version_full}/preprocessed/{model}_coarse_{n_coarsen}.zarr",
                'filter_fluxes_prod': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_filter_prod.zarr", 
                'coarse_fluxes_prod': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_coarse_{n_coarsen}_prod.zarr",
                'filter_fluxes_appendix': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_filter_appendix.zarr", 
                'coarse_fluxes_appendix': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_coarse_{n_coarsen}_appendix.zarr",
                'filter_decomposition_daily': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_filter_decomposed_daily.zarr",
                'filter_decomposition_daily_appendix': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_filter_decomposed_daily_appendix.zarr",
                'coarse_decomposition_daily': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_coarse_decomposed_daily_{n_coarsen}.zarr",
                'filter_decomposition_monthly': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_filter_decomposed_monthly.zarr",
                'coarse_decomposition_monthly': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_coarse_decomposed_monthly_{n_coarsen}.zarr",
                'filter_decomposition_mean': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_filter_decomposed_mean.zarr",
                'coarse_decomposition_mean': f"{bucket}/{project_path}/{version_full}/results/{model}_fluxes_coarse_decomposed_mean_{n_coarsen}.zarr",
            } for model in ['CM26']
        }
    }
    return global_params