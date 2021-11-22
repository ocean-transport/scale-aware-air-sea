# Relevant Literature


- ["Scale of oceanic eddy killing by wind from global satellite observations"](https://advances.sciencemag.org/content/7/28/eabf4920.abstract) by Rai et al., Science Advances (2021)
- ["Disentangling the Mesoscale Ocean-Atmosphere Interactions"](https://agupubs.onlinelibrary.wiley.com/doi/full/10.1029/2018JC014628) by Renault et al., JGR Oceans (2019) 
- ["Ocean mesoscale structure–induced air–sea interaction in a high-resolution coupled model"](https://www.tandfonline.com/doi/full/10.1080/16742834.2019.1569454) by Lin et al., Atmospheric and Oceanic Science Letters (2019)
- ["Western boundary currents regulated by interaction between ocean eddies and the atmosphere"](https://www.nature.com/articles/nature18640) by Ma et al., Nature Letter (2016)
   - they find that ocean mesoscale eddy-atmosphere (OME-A) interaction is important in regulating the dynamics and energetics of western boundary currents (specifically the Kuroshio) in global and regional climate models (CESM and CRCM)
   - OME-A feedback dominates the eddy potential energy destruction
   - their method involves comparing a model control run and a model that uses a spatial low-pass filter of SST to remove the ocean mesoscale effect on the atmosphere

- ["Impact of ocean resolution on coupled air-sea fluxes and large-scale climate"](https://agupubs.onlinelibrary.wiley.com/doi/full/10.1002/2016GL070559) by Roberts et al., _GRL_ (2016)
  - They compare air-sea fluxes in an eddy-permitting (1/4 degree) and eddy-resolving (1/12 degree) run of NEMO (coupled to a 60km resolution MetUM atmosphere), and don't find much of a difference at small scales, but do find a noticeable impact on the mean state
  - They compare correlations between spatially high passed SST, wind stress, and latent heat fields

- ["What Drives Upper-Ocean Temperature Variability in Coupled Climate Models and Observations?"](https://journals.ametsoc.org/view/journals/clim/33/2/jcli-d-19-0295.1.xml) by Small et al., _Journal of Climate_ (2020)
  - They cover a lot in this paper, computing the heat budget in the mixed layer and to 400m depth in a 1-degree and 0.1-degree CESM run
  - They smooth each term in the heat budget of the 0.1-degree model using boxcar averaging and find that they need to smooth to 5-7 degrees to yield similar patterns to the 1-degree, and up to 10 degrees in the boundary currents
