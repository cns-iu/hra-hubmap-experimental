# Imports
import os
import sys
import json

import asyncio
from aiohttp import ClientSession
import requests

from tqdm import tqdm
import time
from datetime import datetime

from config import *
import pandas as pd

async def fetch(url, session):
    async with session.get(url) as response:
        return await response.json()

async def bound_fetch(sem, url, session):
    # Getter function with semaphore.
    async with sem:
        return await fetch(url, session)
    
def get_all_hubmap_dataset_uuids(nexus_token):
    # Published data
    hubmap_metadata_general_url = "https://portal.hubmapconsortium.org/metadata/v0/datasets.tsv"
    df_raw = pd.read_csv(hubmap_metadata_general_url, delimiter='\t')
    published_dataset_uuids = df_raw['uuid'][1:].tolist()

    # Unpublished data
    headers = {
        'Authorization': 'Bearer ' + nexus_token
    }
    params = {
        "format" : "json",
    }

    BASE_URL = "https://entity.api.hubmapconsortium.org/"
    endpoint = f"datasets/unpublished"
    URL = BASE_URL + endpoint
    resp = requests.get(URL, headers=headers, params=params)
    
    unpublished_dataset_uuids = [dt['uuid'] for dt in resp.json()]
    
    return {
        "published_uuids" : published_dataset_uuids,
        "unpublished_uuids" : unpublished_dataset_uuids,
    }

##################################################################################################################################
##################################################################################################################################
# Published data

async def fetch_published_dataset_metadata_parallel(published_dataset_uuids, max_requests_per_sec, nexus_token):
    tasks = []
    sem = asyncio.Semaphore(max_requests_per_sec)

    async with ClientSession() as session:
        for uuid in published_dataset_uuids:    
            url = f"https://portal.hubmapconsortium.org/browse/dataset/{uuid}.json"
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)
            
        responses = asyncio.gather(*tasks)
        return await responses


def get_published_hubmap_metadata_parallel(nexus_token, published_uuids, max_requests_per_sec):    
    all_published_dataset_metadata = []
    # failed_unpublished_fetches = []
    
    for i in tqdm(range(0, len(published_uuids), max_requests_per_sec), desc="Fetching published data parallel"):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(fetch_published_dataset_metadata_parallel(published_uuids[i:i+max_requests_per_sec], max_requests_per_sec, nexus_token))
        all_published_dataset_metadata += loop.run_until_complete(future)
  
    return all_published_dataset_metadata


##################################################################################################################################
##################################################################################################################################
# Unpublished data

async def fetch_unpublished_dataset_metadata_parallel(unpublished_dataset_uuids, max_requests_per_sec, nexus_token):
    tasks = []
    headers = {
        'Authorization': 'Bearer ' + nexus_token
    }
    sem = asyncio.Semaphore(max_requests_per_sec)

    async with ClientSession(headers=headers) as session:
        for uuid in unpublished_dataset_uuids:    
            url = f"https://entity.api.hubmapconsortium.org/entities/{uuid}"
            task = asyncio.ensure_future(bound_fetch(sem, url, session))
            tasks.append(task)
            
        responses = asyncio.gather(*tasks)
        return await responses


def get_unpublished_hubmap_metadata_parallel(nexus_token, unpublished_uuids, max_requests_per_sec):
    all_unpublished_dataset_metadata = []
    # failed_unpublished_fetches = []
    
    for i in tqdm(range(0, len(unpublished_uuids), max_requests_per_sec), desc="Fetching unpublished data parallel"):
        loop = asyncio.get_event_loop()
        future = asyncio.ensure_future(fetch_unpublished_dataset_metadata_parallel(unpublished_uuids[i:i+max_requests_per_sec], max_requests_per_sec, nexus_token))
        all_unpublished_dataset_metadata += loop.run_until_complete(future)
  
    return all_unpublished_dataset_metadata


##################################################################################################################################
##################################################################################################################################


def get_all_hubmap_metadata(filename, max_requests_per_sec, nexus_token):
    dataset_uuids = get_all_hubmap_dataset_uuids(nexus_token)
    
    published_dataset_metadata_list   = get_published_hubmap_metadata_parallel(nexus_token, dataset_uuids["published_uuids"], max_requests_per_sec)
    unpublished_dataset_metadata_list = get_unpublished_hubmap_metadata_parallel(nexus_token, dataset_uuids["unpublished_uuids"], max_requests_per_sec)
    
    published_dataset_metadata   = {dt['uuid'] : dt for dt in published_dataset_metadata_list} 
    unpublished_dataset_metadata = {dt['uuid'] : dt for dt in unpublished_dataset_metadata_list}

    all_dataset_metadata = {
        "published_metadata" : published_dataset_metadata,
        "unpublished_metadata" : unpublished_dataset_metadata,
    }

    with open(f"{filename}_published.json", "w") as outfile:
        json.dump(published_dataset_metadata, outfile)

    with open(f"{filename}_unpublished.json", "w") as outfile:
        json.dump(unpublished_dataset_metadata, outfile)

    with open(f"{filename}_all.json", "w") as outfile:
        json.dump(all_dataset_metadata, outfile)

    

if __name__ == '__main__':
    
    # update the config file to pass the desired parameters.
    filename = configs["output_filename"]
    max_requests_per_sec = configs["max_requests_per_sec"]
    nexus_token = configs["nexus_token"]

    get_all_hubmap_metadata(filename, max_requests_per_sec, nexus_token)

    # Arguments
    # 1. Output Filename
    # 2. Maximum Requests per sec.

    # args = sys.argv
    # assert len(args) <= 2, "The number of arguments should be 2. Ordered as \n1. Output Filename\n2. Maximum Requests per sec"
    
    # now = datetime.now()
    # current_time = now.strftime("%Y_%M_%d_%H_%M_%S")


    # if len(args) == 2:
    #     if isinstance(args[1], str):
    #         filename = args[1]
    #     elif isinstance(args[1], int):
    #         max_requests_per_sec = args[1]
    # elif len(args) == 3:
    #     _, filename, max_requests_per_sec = args
    



