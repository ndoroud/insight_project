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
    return cache
#
#
def nerl_timestamp(tstamp):
    if tstamp[5:10] != "02-29":
        return "2010"+tstamp[4:]
    else:
        return "2010-02-28"+tstamp[10:]
#
#
def insert_values(region,df_row):
    values = region
    for key in df_row.keys():
        values = values + ',' + str(df_row[key])
    return '(' + values + ')'

#
#
"""
def latest_entry(region,sid):
    cur.execute("select timestamp from data order by timestamp desc limit 72")
    last_valid_entry = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', region, 'ghi', 'dni', 'value'], axis='columns', inplace=False)
    last_entry['timestamp'] = pandas.to_datetime(last_entry['timestamp'],infer_datetime_format=True)
    return last_entry#.set_index('timestamp')
"""
#
#
"""
def cache(region):
    cur.execute("select * from cache_{} order by timestamp desc limit 72".format(region))
    cache = pandas.DataFrame(cur.fetchall()).set_axis(['timestamp', 'value','estimate'], axis='columns', inplace=False)
    cache['timestamp'] = pandas.to_datetime(cache['timestamp'],infer_datetime_format=True)
    return cache#.set_index('timestamp')
"""
#
#
###############################################################################
#
#
# Initial run, to do: check if the data and cache tables exist, if yes skip to 
# the update section 
#
#
conn = psycopg2.connect('dbname=main '+psql_settings)
cur = conn.cursor()
#
#
cur.execute("create table data (region varchar(8) not null, time_stamp timestamp not null, demand real,\
    net_generation real, net_generation_solar real, ghi real, dni real, windspeed real, primary key (region, time_stamp))
for temp_region in eba_regions:
    eia_data = pandas.read_csv(filepath_or_buffer="s3://nima-s3/eia/EBA."+temp_region+"-ALL.H.csv").drop("Unnamed: 0",axis=1)
    eia_data['timestamp'] = eia_data['timestamp'].apply(lambda ts: pandas.Timestamp(ts))
    index_range = range(len(eia_data))
    year_range = range(eia_data['timestamp'].iloc[-1].year,eia_data['timestamp'].iloc[0].year)
    #
    data={}
    for yr in year_range:
        nrel_data = nrel(temp_region)
        if pandas.to_datetime(str(yr)).is_leap_year:
            feb29 = pandas.DataFrame(nrel_data['timestamp'][7344:7368])
            feb29['timestamp'] = feb29['timestamp'].apply(lambda ts: ts.replace(year=yr).replace(day=29))
            feb29 = feb29.merge(nrel_data,left_index=True,right_index=True).drop('timestamp_y',axis=1).set_axis(['timestamp','ghi', 'dni','ws'], axis='columns', inplace=False)
            nrel_data = pandas.concat([feb29,nrel_data],ignore_index=True)
            nrel_data['timestamp'] = nrel_data['timestamp'].apply(lambda ts: ts.replace(year=yr))
        else:
            nrel_data['timestamp'] = nrel_data['timestamp'].apply(lambda ts: ts.replace(year=yr))
        data[yr] = eia_data[eia_data['timestamp'].apply(lambda ts: ts.year == yr)].merge(nrel_data,on='timestamp')
    for yr in data.keys():
        for i in range(len(data[yr])):
            cur.execute("INSERT INTO data (region, time_stamp, demand, net_generation, net_generation_solar, ghi, dni, windspeed)\
                        VALUES "+insert_values(temp_region,data[yt].iloc[i]))
        conn.commit()
#
#
cur.close()
conn.close()




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



































