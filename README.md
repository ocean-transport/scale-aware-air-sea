# scale-aware-air-sea
This repository is both an installable package, and a collection of issues/notebooks that document the work done in the ocean-transport group and beyond.
The goal of this project is to quantify the rectified effect of small scale heterogeneity in the atmophere and ocean on the air-sea fluxes computed via bulk formulae.

## Publications
[The Overlooked Sub-Grid Air-Sea Flux in Climate Models (Preprint)](https://eartharxiv.org/repository/dashboard/7144/)

## Quickstart

### Installation
You can install this package by cloning it locally, navigating to the repository and runnning
```
pip install .
```

### Custom Docker image
The science published from the repository is using a [custom Docker image](/Dockerfile) (installing additional dependencies on top of the [pangeo docker image](https://github.com/pangeo-data/pangeo-docker-images)). 
You can find the image on [quay.io](https://quay.io/repository/jbusecke/scale-aware-air-sea?tab=tags) and refer to specific tags used in the notebooks in `./pipeline`.

#### Pulling custom image on LEAP-Pangeo Jupyterhub
To work with the custom image on the LEAP-Pangeo Jupyterhub, just follow the instructions to use a [custom image](), and enter `quay.io/jbusecke/scale-aware-air-sea:<tag>`, where you replace tag with the relevant version tag. 


E.g. for tag `68b654d76dce`:

<img width="687" alt="image" src="https://github.com/user-attachments/assets/c87ad92b-708d-452f-bd1e-c5b48adb51cc">


### How to develop this package
Follow the above instructions but install the package via
```
pip install -e ".[dev]"
```
