# Relevant Literature

## Relevant papers for scale-aware air-sea interaction
- ["Scale of oceanic eddy killing by wind from global satellite observations"](https://advances.sciencemag.org/content/7/28/eabf4920.abstract) by Rai et al., Science Advances (2021)
   - They look at how wind transfers energy to ocean eddies.
   - They find that winds contribute to “ocean eddy killing” (I.e. removes energy from eddies) at length scales of 260 km and shorter
   - Eddy killing happens at essentially all time scales, but has seasonality (peaks in winter)

- ["Disentangling the Mesoscale Ocean-Atmosphere Interactions"](https://agupubs.onlinelibrary.wiley.com/doi/full/10.1029/2018JC014628) by Renault et al., JGR Oceans (2019)
   - They look at how to separate out the current feedback (CFB) from thermal feedback (TFB) of ocean mesoscales on surface stress and winds.
   - They also find that scatterometer data has some biases, primarily since it measures 10m atmosphere winds as “equivalent neutral stability wind” (ENW), which does not correspond to the atmosphere 10m winds in models. Furthermore, the ENW from scatterometers is measured relative to surface currents rather than absolute ENW. The scatterometer wind data may therefore have biases when considering air-sea feedbacks onto surface winds. Surface stress is a better measurement to use from scatterometers.

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

- [“Frontal Scale Air–Sea Interaction in High-Resolution Coupled Climate Models”](https://journals.ametsoc.org/view/journals/clim/23/23/2010jcli3665.1.xml) by Bryan et al, JClim (2010)

- [“Air‐Sea interaction over the Gulf Stream in an ensemble of HighResMIP present climate simulations”](https://link.springer.com/article/10.1007/s00382-020-05573-z) by Bellucci et al, Climate Dynamics (2021)

- [“Coupled Ocean–Atmosphere Covariances in Global Ensemble Simulations: Impact of an Eddy-Resolving Ocean”](https://journals.ametsoc.org/view/journals/mwre/149/5/MWR-D-20-0352.1.xml) by Frolov et al, Monthly Weather Review (2021)
   - Focused on weather-scale forecasts

- ["Local Air-Sea Interactions at Ocean Mesoscale and Submesoscale in a Western Boundary Current"](https://agupubs.onlinelibrary.wiley.com/doi/epdf/10.1029/2021GL097003) by Strobach et al. GRL (2022)
   - Uses ultra high resolution, coupled model (3km ocean, 6km atmosphere) over 3 months of subdaily output
   - Goes in depth about how winds and SST anomalies interact
   - They show that submesoscales are important for producing a secondary circulation where latent heat increases occur over warm SST anomalies during intermittent wind events that can affect beyond the boundary layer (up to 2000km above the boundary layer)
   - The submesoscale is important for creating these secondary circulation events.

- ["Impact of resolution on the atmosphere-ocean coupling along the Gulf Stream in global high resolution models"](https://link.springer.com/content/pdf/10.1007/s00382-021-06098-9.pdf) by E.E. Tsartsali et al. Climate Dynamics (2022)
   - They determine how horizontal resolution affects air-sea interaction over the Gulf Stream, specifically the mechanisms of vertical mixing (VMM) and pressure adjustment (PAM)
   - They use data from 6 models (from PRIMAVERA funded) in HighResMIP, where they classify the models into 4 categories:
      - eddy-parameterized (1 degree ocean, atm 100-250km)
      - eddy permitting (0.25-0.4 degree ocean, 50-100km atm)
      - eddy permitting with increased atmospheric resolution (0.25-0.4 degree ocean, 25-50km atm)
      - eddy resolving - only 1 model (1/12 degree ocean, 50km atm)
   - They also compare to reanalyses and observations
   - They find that eddy-permitting with comparable atmospheric resolution is required to correctly capture the air-sea interaction in the Gulf Stream

### Papers on air-sea fluxes and bulk formulae
- ["Wind and Buoyancy-Forced Upper Ocean"](https://www.pmel.noaa.gov/people/cronin/encycl/ms0157.pdf) by Cronin and Sprintall, Encyclopedia of Ocean Sciences (2001)
   - Great overview of surface heat and wind fluxes

- [“Climatologically Significant Effects of Some Approximations in the Bulk Parameterizations of Turbulent Air–Sea Fluxes”](https://journals.ametsoc.org/view/journals/phoc/47/1/jpo-d-16-0169.1.xml) by Brodeau et al, JPO (2017)
   - They quantify the effects of different bulk formulae and parameter approximations on the exchange of heat, momentum, and freshwater between ocean and atmosphere using an ensemble of sensitivity experiments.
   - Nice intro + general overview of bulk formulae
   - There are 3 common bulk algorithms: COARE (most recent update is 3.0 I think; used in NEMO), NCAR, and ECMWF
   - They find some noticeable disagreements between COARE and NCAR
   - They find that most approximations yield no more than 5% errors, but when multiple errors compile, the bulk formulae can be up to 20% off.
   - They specifically look at climatologies of wind stress, turbulent heat flux, and evaporation.

- [“Bulk Parameterization of Air–Sea Fluxes: Updates and Verification for the COARE Algorithm”](https://journals.ametsoc.org/view/journals/clim/16/4/1520-0442_2003_016_0571_bpoasf_2.0.co_2.xml) by Fairall et al, JClim (2003)
   - Details about the COARE algorithm for computing bulk formulae of air-sea fluxes
   - COARE Algorithm is used in NEMO ocean model
