#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
January 2020
@author: nima

This script requests the Demand, Net Generation and Net Generation Solar hourly grid data from the EIA's API
and stores the result in CSV files in our S3 bucket.
"""
#############################################################################################################################################
#
#
import os
import boto3
import io
import pandas
import requests
from datetime import datetime
from functools import reduce
#
# Import environment variables, project_dir is the local directory where the repository is cloned, the name of the s3bucket to use
# and the PostgreSQL database information (host, credentials) which are combined and stored as psql_settings.
#
project_dir = os.getenv("project_dir")
nrel_key = os.getenv("nrel_key")
eia_key = os.getenv("eia_key")
s3bucket = os.getenv("s3_bucket")
psql_h = os.getenv("psql_h")
psql_u = os.getenv("psql_u")
psql_p = os.getenv("psql_p")
psql_settings = "host={} user={} password={}".format(psql_h,psql_u,psql_p)
#
#
# Define s3 for convenience access to s3 storage.
#
s3 = boto3.resource('s3')
#
#
#
# The list of grid regions and the states they contain to be used in for loops.
#
eba_regions = []
with open(project_dir+"/lists/eba_regions.csv","r") as ebaregions:
    for line in ebaregions:
        reg = line.strip().split(",")
        eba_regions.append(reg[0])
#
#
# Columns of the EIA data to include.
#
siddict = {"demand":"D","net-generation":"NG","net-generation-solar":"NG.SUN"}
#
# EIA's api link.
#
eia_api = "http://api.eia.gov/series/"
#
#
#
#
#############################################################################################################################################
#
#
# Current time to include with the changes to the database as well as for logging.
#
def current_time(fr="T"):
    return pandas.to_datetime(datetime.now()).round(freq = fr)
#
#
# API parameters to include in the request, returns a dictionary.
#
def parameters(region,series):
    return {"api_key": eia_key,"series_id": "EBA."+region+"-ALL."+siddict[series]+".H","out": "json"}
#
#
# Makes the API call with the parameters for the grid region and the desired EIA column (series).
#
def EIA_call(region,ser):
    return requests.get(eia_api,parameters(region,ser))
#
#
# Uses EIA_call and translates the response into a pandas dataframe.
#
def EIA_to_df(region,ser):
    data = pandas.DataFrame(EIA_call(region,ser).json()['series'][0]['data']).set_axis(['timestamp', ser], axis='columns', inplace=False)
    data['timestamp'] = pandas.to_datetime(data['timestamp'],infer_datetime_format=True)
    return data
#
#
#############################################################################################################################################
#
#
# Make API calls and store the resluts on S3.
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
    s3.Object(s3bucket[5:], "eia/EBA."+region+"-ALL.H.csv").put(Body=csv_buffer.getvalue())
    del data
end_time = str(current_time("s"))

#
# Log:
with open(project_dir+"/logs/eia_logs.csv","a") as log_file:
    log_file.write(start_time+", "+end_time+"\n")
#
#
#
#############################################################################################################################################
#############################################################################################################################################
#############################################################################################################################################
