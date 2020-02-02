#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
January 2020
@author: nima

Notes:
    -leap years (2016, 2020)
    -
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
from functools import reduce
#
# Call environment variables
project_dir = os.getenv("project_dir")
#
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
#
#
###############################################################################
#
#
def nrel(region):
    cur.execute("select * from nrel_{} order by timestamp desc".format(region))
    cache = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', 'ghi', 'dni', "ws"], axis='columns', inplace=False)
    cache['timestamp'] = pandas.to_datetime(cache['timestamp'],infer_datetime_format=True)
    return cache#.set_index('timestamp')
#
#
def nerl_timestamp(tstamp):
    if tstamp[5:10] != "02-29":
        return "2010"+tstamp[4:]
    else:
        return "2010-02-28"+tstamp[10:]
#
#
def latest_entry():
    cur.execute("select timestamp from data order by timestamp desc limit 1")
    last_entry = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', region, 'ghi', 'dni', 'value'], axis='columns', inplace=False)
    last_entry['timestamp'] = pandas.to_datetime(last_entry['timestamp'],infer_datetime_format=True)
    return last_entry#.set_index('timestamp')
#
#
def cache(region):
    cur.execute("select * from cache_{} order by timestamp desc limit 72".format(region))
    cache = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', 'value','estimate'], axis='columns', inplace=False)
    cache['timestamp'] = pandas.to_datetime(cache['timestamp'],infer_datetime_format=True)
    return cache#.set_index('timestamp')
#
#
###############################################################################
#
#
#temp_region = input()
temp_region = "NE"
eia_data = pandas.read_csv(filepath_or_buffer="s3://nima-s3/eia/EBA."+temp_region+"-ALL.H.csv").drop("Unnamed: 0",axis=1)
# eia_data = eia_data[eia_data['timestamp'] > latest_entry(region)]
eia_data = eia_data[eia_data['timestamp'] > "1900-01-01 00:00:00"]
index_range = len(eia_data)








"""
cur.execute("create table data (timestamp timestamp not null, region varchar(50) not null, demand int,\
    net-generation int, net-generation-solar int, ghi int, dni int, windspeed int)
conn = psycopg2.connect('dbname=main '+psql_settings)
cur = conn.cursor()
#
#
#temp_region = input()
temp_region = "CAL"
#
#
x = nrel('car')
#
#
cur.close()
conn.close()
"""



































