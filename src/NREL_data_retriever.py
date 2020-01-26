#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 23 20:16:01 2020

@author: nima
"""
#
#
import requests
import pandas
#import json
#
#
nrel_key_file = "/home/nima/Data/project/keys/nrel.key"
data_filename = "/home/nima/Data/project/nrel_data.json"
#
#
# Input API link and API key.
#
nrel_api = "https://developer.nrel.gov/api/solar/data_query/v1.json?"
with open(nrel_key_file,"r") as key:
    nrel_key=key.read()
#
#
# Set the parameters for the API call.
#
parameters = {"api_key": nrel_key,
              "lat": "38",
              "lon": "-120",
              "radius": "150",
              "dataset": "tmy2"}
#
#
# Make API call.
#
response = requests.get(nrel_api,parameters)
data = response.json()
#pandas.read_json(filepath_or_buffer=data)
#
#
#file = open(data_filename,"w")
#with open(data_filename, 'w') as outfile:
#    json.dump(data, outfile)