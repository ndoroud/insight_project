#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
January 2020
@author: nima
"""
###############################################################################
#
#
import os
import boto3
import io
#import json
import pandas
import requests
#import psycopg2
from datetime import datetime
from functools import reduce
#
# Call environment variables
project_dir = os.getenv("project_dir")
#
nrel_key = os.getenv("nrel_key")
eia_key = os.getenv("eia_key")
census_key = os.getenv("census_key")
#
psql_h = os.getenv("psql_h")
psql_u = os.getenv("psql_u")
psql_p = os.getenv("psql_p")
psql_settings = "host={} user={} password={}".format(psql_h,psql_u,psql_p)
#
#
s3 = boto3.resource('s3')
#
#
#
#
eba_regions = []
with open(project_dir+"/lists/eba_regions.csv","r") as ebaregions:
    for line in ebaregions:
        reg = line.strip().split(",")
        eba_regions.append(reg[0])
#
#
siddict = {"demand":"D","net-generation":"NG","net-generation-solar":"NG.SUN"}
#
#
eia_api = "http://api.eia.gov/series/"
#
#
#
#
###############################################################################
#
#
def current_time(fr="T"):
    return pandas.to_datetime(datetime.now()).round(freq = fr)
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
###############################################################################
#
#
#inflow = input()
#region = inflow
#
# Make API calls.
#
start_time = str(current_time("s"))
for region in eba_regions:
    temp_data = []
    for ser in siddict.keys():
        temp_data.append(EIA_to_df(region,ser))
    data = reduce(lambda left,right: pandas.merge(left,right,how='outer',on='timestamp'), temp_data)
#
#
    csv_buffer = io.StringIO()
    data.to_csv(csv_buffer)
    s3.Object("nima-s3", "eia/EBA."+region+"-ALL.H.csv").put(Body=csv_buffer.getvalue())
    del data
end_time = str(current_time("s"))

#
# Log:
with open(project_dir+"/logs/eia_logs.csv","a") as log_file:
    log_file.write(start_time+", "+end_time+"\n")
#
#
#
"""
# Alternatively we store different series in separate files on S3
#
#
for region in eba_regions:
    for ser in siddict.keys():
        temp_data = EIA_to_df(region,ser)
        csv_buffer = io.StringIO()
        temp_data.to_csv(csv_buffer)
        s3.Object("nima-s3", "eia/EBA."+region+"-ALL."+siddict[ser]+".H.csv").put(Body=csv_buffer.getvalue())
        del temp_data
"""