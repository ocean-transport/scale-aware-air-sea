FROM quay.io/pangeo/pangeo-notebook:ebeb9dd
LABEL maintainer="Julius Busecked"
LABEL repo="https://github.com/ocean-transport/scale-aware-air-sea"


RUN mamba install aerobulk-python -y 
RUN pip install coiled
