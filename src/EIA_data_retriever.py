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
import requests
# import psycopg2
# from functools import reduce
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
siddict = {"demand":"D","net-generation":"NG","net-generation-solar":"NG.SUN"}
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
#
#
def parameters(region,series):
    return {"api_key": eia_key,"series_id": "EBA."+region+"-ALL."+siddict[series]+".H","out": "json"}
#
#
def EIA_call(region,ser):
    return requests.get(eia_api,parameters(region,ser))
#
#
def EIA_to_df(region,ser):
    data = pandas.DataFrame(EIA_call(region,ser).json()['series'][0]['data']).set_axis(['timestamp', region+"_"+ser], axis='columns', inplace=False)
    data['timestamp'] = pandas.to_datetime(data['timestamp'],infer_datetime_format=True)
    return data
#
#
#data = EIA_to_df(EIA_call())
#
# Make API calls.
#
for region in eba_regions.keys():
    for ser in siddict.keys():
        temp_data = EIA_to_df(region,ser)
        csv_buffer = io.StringIO()
        temp_data.to_csv(csv_buffer)
        s3.Object("nima-s3", "eia/EBA."+region+"-ALL."+siddict[ser]+".H.csv").put(Body=csv_buffer.getvalue())
        del temp_data
#
#
"""
#
# Alternatively we can combine the data first and store it on S3. Uncomment line 16 for "reduce" function.
#
#
for region in eba_regions.keys():
    temp_data = []
    for ser in siddict.keys():
        temp_data.append(EIA_to_df(region,ser))
    data = reduce(lambda left,right: pandas.merge(left,right,how='outer',on='timestamp'), temp_data)
#
#
    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer)
    s3.Object("nima-s3", "eia/EBA."+region+"-ALL.H.csv").put(Body=csv_buffer.getvalue())
    data.del()
"""