# hra-hubmap-experimental

This repository covers the work of acquiring the HuBMAP metadata and generating experimental data mapping to the HRA. 

Contents:

    .
    ├── config.py                                           # configurations file - updated (as of 03/06/2023)
    ├── generate_tables.ipynb                               # ipynb notebook file to generate experimental tables
    ├── main.py                                             # main runner file to generate the HuBMAP datasets metadata json files.
    ├── hubmap_auto.ipynb                                   # ipynb file to perform EDA/building the code.
    ├── hubmap_ctpop.ipynb                                  # ipynb file to generate the files for the CT_Pop paper.
    ├── OMAP data in HuBMAP Data Portal.csv                 # 
    └── Valid Azimuth Organs and Assay Types.csv            #
    

## Setting up the environment
- Python 3.7 or later
- Install packages:
  - `aiohttp` : 3.8.1
  - `tqdm` : 4.59.0


## Executing the code

Before running the main.py code, the config files needs to be checked for any updates. 
1. `output_filename` : desired output filename
2. `max_requests_per_sec` : the API calls are made in parallel. The default is set to 100.
3. `nexus_token` : HuBMAP Nexus token. (Nexus token regenerates every 3 days for a HuBMAP registered account)


```
python main.py
```

  
  
 
