#keep this until aerobulk-python runs with py3.10/3.11 https://github.com/conda-forge/aerobulk-python-feedstock/pull/14
FROM quay.io/pangeo/pangeo-notebook:ebeb9dd 
LABEL maintainer="Julius Busecked"
LABEL repo="https://github.com/ocean-transport/scale-aware-air-sea"


RUN mamba install -n=notebook aerobulk-python -y 
RUN pip install coiled
