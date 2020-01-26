#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 09:49:18 2020

@author: nima
"""
#
#
import requests
import json
#
#
sid = "EBA.CAL-ALL.D.H" #input()
eia_key_file = "/home/nima/Data/project/keys/eia.key"
data_filename = "/home/nima/Data/project/EIA/"+sid+".json"
#
#
# Input API link and API key.
#
eia_api = "http://api.eia.gov/series/"
with open(eia_key_file,"r") as key:
    eia_key=key.read()
#
#
# Set the parameters for the API call.
#
parameters = {"api_key": eia_key,
              "series_id": str(sid),
              "out": "json"}
#
#
# Make API call.
#
response = requests.get(eia_api,parameters)
data = response.json()
#
#
file = open(data_filename,"w")
with open(data_filename, 'w') as outfile:
    json.dump(data, outfile)