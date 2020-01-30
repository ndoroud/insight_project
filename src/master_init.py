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
import boto3
import psycopg2
import json
import pandas
import datetime
from io import StringIO
from functools import reduce
# import requests
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
settings = "dbname=test"
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
siddict = {"demand":"D","net-generation-solar":"NG.SUN","net-generation":"NG"}
#
#
#
#
###############################################################################
#
#
def latest_entry(region):
    cur.execute("select * from data_{} order by timestamp desc limit 1".format(region))
    last_entry = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', 'dni','value'], axis='columns', inplace=False)
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
def nrel(region):
    cur.execute("select * from nrel_{} order by timestamp desc".format(region))
    cache = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', 'dni'], axis='columns', inplace=False)
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
###############################################################################
#
# Query the database for the latest enteries in the main table and
# rolling window table
#
conn = psycopg2.connect(settings)
cur = conn.cursor()
#
#
for region in eba_regions.keys():
#
    nrel_data = nrel(region)
    eia_data = pandas.read_csv(filepath_or_buffer="s3://nima-s3/eia/EBA."+region+"-ALL.D.H.csv").drop("Unnamed: 0",axis=1)
#   eia_data = eia_data[eia_data['timestamp'] > latest_entry(region)]
    eia_data = eia_data[eia_data['timestamp'] > "1900-01-01 00:00:00"]
    index_range = len(eia_data)
#
    if index_range > 72 :
        for i in eia_data[72:].index:
            time_stamp = eia_data["timestamp"][i]
            nl = nrel_data.index[nrel_data["timestamp"] == nerl_timestamp(time_stamp)].tolist()[0]
            cur.execute("INSERT INTO data_{} (timestamp, dni, demand) VALUES \
                        ('{}','{}','{}')".format(region,time_stamp,nrel_data["dni"][nl],eia_data[region+"_demand"][i]))
        conn.commit()
    else:
        pass
    del nrel_data
#
#   cache = cache(region)
#   latest_cache_entry = cache["timestamp"][0]
    lcei = 72 #eia_data.index[eia_data["timestamp"] == latest_cache_entry].tolist()[0]
    for i in eia_data[0:lcei].index:
        time_stamp = eia_data["timestamp"][i]
        cur.execute("INSERT INTO cache_{} (timestamp, estimate) VALUES \
                    ('{}','{}')".format(region,time_stamp,eia_data[region+"_demand"][i]))
        conn.commit()
#    if lcei < 72 :
#        for i in eia_data[lcei:72].index:
#            time_stamp = eia_data["timestamp"][i]
#            cache_index = cache.index[cache["timestamp"] == time_stamp].tolist()[0]
#            if cache["estimate"][cache_index] != eia_data[region+"_demand"][i]:
#                cur.execute("UPDATE cache_{} SET actual='{}' where timestamp='{}'".format(region,eia_data[region+"_demand"][i],time_stamp))
#            else:
#                pass
#        conn.commit()
#    else:
#        pass
#
#
cur.close()
conn.close()
"""
conn = psycopg2.connect(settings)
cur = conn.cursor()
#
#
for region in eba_regions.keys():
    cur.execute("create table data_{} (timestamp timestamp primary key, dni int not null, demand int not null)".format(region))
    cur.execute("create table cache_{} (timestamp timestamp primary key, estimate int not null, actual int)".format(region))
    conn.commit()
#
#
cur.close()
conn.close()
"""