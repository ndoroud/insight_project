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
    return last_entry.set_index('timestamp')
#
#
def cache(region):
    cur.execute("select * from cache_{} order by timestamp desc limit 72".format(region))
    cache = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', 'value','estimate'], axis='columns', inplace=False)
    cache['timestamp'] = pandas.to_datetime(cache['timestamp'],infer_datetime_format=True)
    return cache.set_index('timestamp')
#
#
def nrel(region):
    cur.execute("select * from nrel_{} order by timestamp desc".format(region))
    cache = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', 'value','estimate'], axis='columns', inplace=False)
    cache['timestamp'] = pandas.to_datetime(cache['timestamp'],infer_datetime_format=True)
    return cache.set_index('timestamp')
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
    cur.execute("create table data_{} (timestamp timestamp primary key, dni int not null, demand int not null)".format(region))
    cur.execute("create table cache_{} (timestamp timestamp primary key, estimate int not null, actual int)".format(region))
conn.commit()
#
#
cur.close()
conn.close()