#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
January 2020
@author: nima
"""
###############################################################################
#
#
import boto3
import io
import json
import pandas
import psycopg2
import requests
#
#
s3 = boto3.resource('s3')
#
#
with open("project_directory.txt","r") as pdir:
    project_folder = pdir.read().strip()
#
#
with open(project_folder+"/keys/psql.json","r") as file:
    psql_settings = json.load(file)
#
#
settings = ""
for key in psql_settings.keys():
    settings = settings +" "+ key + "=" + psql_settings[key]
#
#
eba_regions = {}
with open(project_folder+"/lists/eba_regions.csv","r") as ebaregions:
    for line in ebaregions:
        reg = line.strip().split(",")
        eba_regions[reg[0]] = reg[1:]
#
#
eia_api = "http://api.eia.gov/series/"
with open(project_folder+"/keys/eia.key","r") as key:
    eia_key=key.read()
#
#
#
#
###############################################################################
#
#
siddict = {"demand":"D","net-generation-solar":"NG.SUN","net-generation":"NG"}
#
#
def parameters(region,series):
    return {"api_key": eia_key,"series_id": "EBA."+region+"-ALL."+siddict[series]+".H","out": "json"}
#
#
#
# Make API call.
#
for ser in siddict.keys():
    for region in eba_regions.keys():
        response = requests.get(eia_api,parameters(region,ser))
        data = pandas.DataFrame(response.json()['series'][0]['data']).set_axis(['timestamp', 'demand'], axis='columns', inplace=False)
#
#
        data['timestamp'] = pandas.to_datetime(data['timestamp'],infer_datetime_format=True)
        data = data.set_index('timestamp')
#
#
        csv_buffer = io.StringIO()
        data.to_csv(csv_buffer)
        s3.Object("nima-s3", "eia/EBA."+region+"-ALL."+siddict[ser]+".H.csv").put(Body=csv_buffer.getvalue())
#
#
