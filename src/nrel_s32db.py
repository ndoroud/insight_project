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
import json
import pandas
import requests
import psycopg2
from datetime import datetime
from datetime import timedelta
from functools import reduce
#
# Call environment variables
project_dir = os.getenv("project_dir")
#
#nrel_key = os.getenv("nrel_key")
#eia_key = os.getenv("eia_key")
#census_key = os.getenv("census_key")
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
def current_time(fr="T"):
    return pandas.to_datetime(datetime.now()).round(freq = fr)
#
#
#
#
###############################################################################
#
# Import lists as dictionaries
#
states = {}
with open(project_dir+"/lists/list_of_states.csv","r") as listofstates:
    for line in listofstates:
        state = line.strip().split(",")
        states[state[0]] = state[1]
#
#
eba_regions = {}
with open(project_dir+"/lists/eba_regions.csv","r") as ebaregions:
    for line in ebaregions:
        region = line.strip().split(",")
        eba_regions[region[0]] = region[1:]
#
#
nrel_headers = {}
with open(project_dir+"/lists/nrel_headers.csv","r") as nrelheaders:
    for line in nrelheaders:
        header = line.strip().split(",")
        if header[2] in nrel_headers.keys():
            nrel_headers[header[2]].append(header)
        else:
            nrel_headers[header[2]] = [header]
#
#
nrel_keys = {"date": 'Date (MM/DD/YYYY)',
             "time": 'Time (HH:MM)',
             "ghi": 'GHI (W/m^2)',
             "dni": 'DNI (W/m^2)',
             "wind": 'Wspd (m/s)'}
#
#
###############################################################################
#
# Define functions to import and reduce the nrel data for each eba_region
#
# stations(r) outputs the list of file_ids for the stations situated in the
# eba region r
#
def stations(region):
    files = []
    for state in eba_regions[region]:
        for entry in nrel_headers[state]:
            files.append(entry[0])
    return files
#
#
# to_datetime24 take a timestamp in the MM/DD/YYYYHH:MM (24:00) format, sets
# the year to 2010 and converts timestamp(y=2010) to pandas datetime
#
def to_datetime24(timestamp,year='2010'):
    if timestamp[10:12] != '24':
        timestamp = timestamp[0:6] + year + timestamp[10:]
        return pandas.to_datetime(timestamp, format='%m/%d/%Y%H:%M')
    elif timestamp[0:5] != '12/31':
        timestamp = timestamp[0:6] + year +'00' + timestamp[12:]
        return pandas.to_datetime(timestamp, format='%m/%d/%Y%H:%M') + datetime.timedelta(days=1)
    else:
        timestamp = timestamp[0:6] + year +'00' + timestamp[12:]
        return pandas.to_datetime(timestamp, format='%m/%d/%Y%H:%M') + datetime.timedelta(days=-364)
#
#
def from_s3(file_id):
    return pandas.read_csv(StringIO("\n".join(s3.Object(bucket_name='nima-s3', \
            key="nrel/"+file_id+"TYA.CSV").get()['Body'].read().decode("utf-8").split("\r\n")[1:])))
#
#
def dni_data(file_id):
    data = from_s3(file_id)[list(nrel_keys.values())].set_axis(['timestamp', 'time','dni '+file_id], axis='columns', inplace=False)
    data['timestamp'] = (data['timestamp']+data['time']).apply(to_datetime24)
    return data.drop('time',axis=1).set_index('timestamp')
#
#
conn = psycopg2.connect(settings)
cur = conn.cursor()
for region in eba_regions.keys():
    region_data = []
    cur.execute("CREATE TABLE nrel_{} (timestamp timestamp primary key, DNI int not null)".format(region))
    for st in stations("CAR"):
        region_data.append(dni_data(st))
    region_data = reduce(lambda  left,right: pandas.merge(left,right,on=['timestamp']), region_data)
    region_data = region_data.mean(axis=1)
    for i in region_data.index:
        cur.execute("INSERT INTO nrel_{} (timestamp, dni) VALUES \
                    ('{}','{}')".format(region,region_data[[i]].keys()[0],int(region_data[i])))
    conn.commit()
    del region_data
cur.close()
conn.close()

###############################################################################   
###############################################################################
###############################################################################