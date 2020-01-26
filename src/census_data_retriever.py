#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21, 2020

@author: nima
"""
#
#
import requests
census_key_file = "/home/nima/Data/project/keys/census.key"
#
census_api = "https://api.census.gov/data/2010/dec/sf1"
with open(census_key_file,"r") as key:
    census_key=key.read()
#
parameters = {"key": census_key,
              "get": "P001001,NAME",
              "for": "state:06"}
#
response = requests.get(census_api,parameters)
data = response.json()
