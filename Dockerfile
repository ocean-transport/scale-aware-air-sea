FROM quay.io/pangeo/pangeo-notebook:2023.10.24 
LABEL maintainer="Julius Busecked"
LABEL repo="https://github.com/ocean-transport/scale-aware-air-sea"


RUN mamba install -n=notebook aerobulk-python -y 
RUN pip install coiled
