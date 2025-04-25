def get_params(version:str, test:bool=True) -> dict[str, str]:
    bucket = 'scale-aware-air-sea/jbusecke' # equivalent to os.environ['PERSISTENT_BUCKET'], but this should work for all collaborators
    bucket_pub = 'leap-pubs/busecke_balwada_et_al_GRL'
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
                    'preprocessing':
                        {
                            'scratch': f"{scratch}/{version_full}/temp/{model}.zarr"
                        },
                    'smoothing':
                        {
                        'filter': f"{bucket}/{version_full}/smoothing/{model}_filter.zarr",
                        'coarse': f"{bucket}/{version_full}/smoothing/{model}_coarse_{n_coarsen}.zarr",
                        },
                    'fluxes':
                        {
                        'filter':
                         {
                             'prod': f"{bucket}/{version_full}/fluxes/{model}_fluxes_filter_prod.zarr", 
                             'appendix': f"{bucket}/{version_full}/fluxes/{model}_fluxes_filter_appendix.zarr",
                         },
                         'coarse':
                         {
                            'prod': f"{bucket}/{version_full}/fluxes/{model}_fluxes_coarse_{n_coarsen}_prod.zarr",
                            'appendix': f"{bucket}/{version_full}/fluxes/{model}_fluxes_coarse_{n_coarsen}_appendix.zarr",
                         },
                         },
                 'results':
                 {
                    'filter':{
                         'native':{
                             'prod':f"{bucket}/{version_full}/results/{model}_fluxes_filter_decomposed_native_prod.zarr",
                             'appendix':f"{bucket}/{version_full}/results/{model}_fluxes_filter_decomposed_native_appendix.zarr",
                             'all_terms':f"{bucket}/{version_full}/results/{model}_fluxes_filter_decomposed_native_all_terms.zarr",
                         },
                         'mean':{
                             'prod':f"{bucket}/{version_full}/results/{model}_fluxes_filter_decomposed_mean_prod.zarr",
                             'appendix':f"{bucket}/{version_full}/results/{model}_fluxes_filter_decomposed_mean_appendix.zarr",
                             'all_terms':f"{bucket}/{version_full}/results/{model}_fluxes_filter_decomposed_mean_all_terms.zarr",
                         },
                     
                     },
                     'coarse':{
                         'native':{
                             'prod':f"{bucket}/{version_full}/results/{model}_fluxes_coarse_decomposed_native_prod_{n_coarsen}.zarr",
                             'appendix':f"{bucket}/{version_full}/results/{model}_fluxes_coarse_decomposed_native_appendix_{n_coarsen}.zarr",
                             'all_terms':f"{bucket}/{version_full}/results/{model}_fluxes_coarse_decomposed_native_all_terms.zarr",
                         },
                         'mean':{
                             'prod':f"{bucket}/{version_full}/results/{model}_fluxes_coarse_decomposed_mean_prod_{n_coarsen}.zarr",
                             'appendix':f"{bucket}/{version_full}/results/{model}_fluxes_coarse_decomposed_mean_appendix_{n_coarsen}.zarr",
                             'all_terms':f"{bucket}/{version_full}/results/{model}_fluxes_coarse_decomposed_mean_all_terms.zarr",
                         },
                     
                     },
                },
                'plotting':
                {
                    'max_ice_mask': f"{bucket}/{version_full}/plotting/{model}_max_ice_mask.zarr",
                    'full_fluxes':{ 
                        k: {
                            kk:f"{bucket}/{version_full}/plotting/{model}_full_flux_{k}_{kk}.zarr" for kk in ['global_mean', 'time_mean']
                        } for k in ['online', 'offline']
                    },
                },
                'plotting_pub':
                {
                    'max_ice_mask': f"{bucket_pub}/{version_full}/plotting/{model}_max_ice_mask.zarr",
                    'full_fluxes':{ 
                        k: {
                            kk:f"{bucket}/{version_full}/plotting/{model}_full_flux_{k}_{kk}.zarr" for kk in ['global_mean', 'time_mean']
                        } for k in ['online', 'offline']
                    },
                },
            } for model in ['CM26','CESM']
        }
    }
    return global_params
