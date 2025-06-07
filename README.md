# scale-aware-air-sea
This repository is both an installable package, and a collection of issues/notebooks that document the work done in the ocean-transport group and beyond.
The goal of this project is to quantify the rectified effect of small scale heterogeneity in the atmophere and ocean on the air-sea fluxes computed via bulk formulae. See [below](#publications) for relevant publications.


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

## Publications
[The Overlooked Sub-Grid Air-Sea Flux in Climate Models (Preprint)](https://eartharxiv.org/repository/dashboard/7144/)

### Publication relevant parts of the repo

```
.
├── paper [Code used to produce the GRL Paper plots]
├── pipeline [Raw processing code (smoothing + offline flux computation)]
├── scale_aware_air_sea [installable software package]
```
> Ommited folders are either part of the installable package, or testing notebooks and other non relevant code.

### Data Availability
The raw data is available as a mix of egress-free and requesters pays data (see `scale-aware-air-sea/stages.py` for detailed paths).

All aggregated data used for plotting is available on the [M²LInES](https://m2lines.github.io/) funded [Open Storage Network Pod](https://leap-stc.github.io/reference/infrastructure.html?highlight=m2lines#m2lines-osn-pod) in egress free object storage at `https://nyu1.osn.mghpcc.org/leap-pubs/busecke_balwada_et_al_GRL/v1.0.1/plotting`.

### Reproduce Results
[![Publication Code](https://zenodo.org/badge/DOI/10.5281/zenodo.15615679.svg)](https://doi.org/10.5281/zenodo.15615679)

To reproduce the results from the 2025 GRL Paper **exactly**, check out the code at the `v1.1.0` tag (which corresponds to the final code+figures after revision), and run the `paper/full_plots-submission.ipynb` notebook within the `quay.io/jbusecke/scale-aware-air-sea:68b654d76dce` docker image (find some guidance how to do this locally [here](https://juliusbusecke.com/blog/posts/pangeo-docker-local/) or follow the advice for a 2i2c JupyterHub below).

>[!WARNING]
> Even the picture processing is fairly slow. Be prepared to take a coffee break if you are running this on a smaller laptop.
