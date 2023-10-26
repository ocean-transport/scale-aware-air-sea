FROM quay.io/pangeo/pangeo-notebook:2023.08.29
LABEL maintainer="Julius Busecked"
LABEL repo="https://github.com/ocean-transport/scale-aware-air-sea"


RUN conda activate notebook && mamba install aerobulk-python -y && pip install coiled
