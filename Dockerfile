FROM quay.io/pangeo/pangeo-notebook:ebeb9dd
LABEL maintainer="Julius Busecked"
LABEL repo="https://github.com/ocean-transport/scale-aware-air-sea"


RUN mamba install -n=notebook aerobulk-python -y 
RUN pip install coiled
