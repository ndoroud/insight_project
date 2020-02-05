#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
January 2020
@author: nima

Notes:
    -leap years (2016, 2020)  -check!
    -Check for missing data
"""
###############################################################################
#
#
import os
import boto3
#import io
#import json
import pandas
#import requests
import psycopg2
from datetime import datetime
from datetime import timedelta
#from functools import reduce
from sqlalchemy import create_engine
#
# Call environment variables
project_dir = os.getenv("project_dir")
#
#
psql_h = os.getenv("psql_h")
psql_u = os.getenv("psql_u")
psql_p = os.getenv("psql_p")
psql_settings = "host={} user={} password={}".format(psql_h,psql_u,psql_p)
psql_engine = create_engine('postgresql://'+psql_u+':'+psql_p+'@'+psql_h+':5432/main')
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
    values = "'{}'".format(region)
    for key in df_row.keys():
        values = values + ',' + "'{}'".format(df_row[key])
    return values
#
#
def fetch_main(region,n="72"):
    return pandas.read_sql("select * from data where region like '{}' order by time_stamp desc limit \
                           {}".format(region,n),conn).drop(['region','ghi','dni','windspeed'],axis=1)
#
#
def fetch_recent(region,window):
    eia_data = pandas.read_csv(filepath_or_buffer="s3://nima-s3/eia/EBA."+temp_region+"-ALL.H.csv").drop("Unnamed: 0",axis=1)
    eia_data.drop(eia_data.tail(2).index,inplace=True)
    eia_data['timestamp'] = eia_data['timestamp'].apply(lambda ts: pandas.Timestamp(ts))
    return eia_data[eia_data['timestamp'] >= window]
#
#
def merge_data(eia_data,temp_region,yr):
    nrel_data = nrel(temp_region)
    if pandas.to_datetime(str(yr)).is_leap_year:
        feb29 = pandas.DataFrame(nrel_data['timestamp'][7344:7368])
        feb29['timestamp'] = feb29['timestamp'].apply(lambda ts: ts.replace(year=yr).replace(day=29))
        feb29 = feb29.merge(nrel_data,left_index=True,right_index=True).drop('timestamp_y',axis=1).set_axis(['timestamp','ghi', 'dni','ws'], axis='columns', inplace=False)
        nrel_data = pandas.concat([feb29,nrel_data],ignore_index=True)
        nrel_data['timestamp'] = nrel_data['timestamp'].apply(lambda ts: ts.replace(year=yr))
    else:
        nrel_data['timestamp'] = nrel_data['timestamp'].apply(lambda ts: ts.replace(year=yr))
    return eia_data[eia_data['timestamp'].apply(lambda ts: ts.year == yr)].merge(nrel_data,on='timestamp')
#
#
def insert_into_db(eia_data,temp_region,yr):
    temp_data = merge_data(eia_data,temp_region,yr)
    for i in range(len(temp_data)):
        cur.execute("INSERT INTO data (region, time_stamp, demand, net_generation, net_generation_solar, ghi, dni, windspeed, \
                    updated) VALUES ("+insert_values(temp_region,temp_data.iloc[i])+",'{}'".format(update_time)+")")
    return None
#
#
def recent_eq_main(ts):
    r_entry = recent_entries[recent_entries['timestamp'] == ts].drop('timestamp',axis=1).set_axis(['demand','net_generation', 'net_generation_solar'], axis='columns', inplace=False)
    m_entry = main_entries[main_entries['time_stamp'] == ts].drop(['time_stamp','updated'],axis=1)
    for key in r_entry.keys():
        eq = True
        if not r_entry[key].isnull().iloc[0]:
            eq = eq and r_entry[key].iloc[0] == m_entry[key].iloc[0]
        else:
            pass
    return eq
#
#
def update_row(region,ts):
    re = recent_entries[recent_entries['timestamp'] == ts].iloc[0]
    cur.execute("update data set (demand,net_generation,net_generation_solar,updated)=('{}','{}','{}','{}') where region='{}' \
                            and time_stamp='{}'".format(re['demand'],re['net-generation'],re['net-generation-solar'],update_time,region,ts))
    return None
#
#
def change_history(region,ts):
    r_entry = recent_entries[recent_entries['timestamp'] == ts].drop('timestamp',axis=1).set_axis(['demand','net_generation', 'net_generation_solar'], axis='columns', inplace=False)
    m_entry = main_entries[main_entries['time_stamp'] == ts].drop(['time_stamp','updated'],axis=1)
    re = {}
    for key in r_entry.keys():
        if not r_entry[key].isnull().iloc[0] and not m_entry[key].isnull().iloc[0]:
            re[key] = r_entry[key].iloc[0] - m_entry[key].iloc[0]
        else:
            re[key] = r_entry[key].iloc[0]
    cur.execute("INSERT INTO data_history (region,time_stamp,demand,net_generation,net_generation_solar,updated) VALUES \
                ('{}','{}','{}','{}','{}','{}')".format(region,ts,re['demand'],re['net-generation'],re['net-generation-solar'],update_time))
#
#
def init_db():
    cur.execute("create table data (region varchar(8) not null, time_stamp timestamp not null, demand real,\
                net_generation real, net_generation_solar real, ghi real, dni real, windspeed real,\
                    updated timestamp not null, primary key (region, time_stamp))")
    cur.execute("create table data_history (region varchar(8) not null, time_stamp timestamp not null, demand real,\
                net_generation real, net_generation_solar real, updated timestamp not null, \
                    primary key (region, time_stamp, updated), foreign key (region, time_stamp) references data(region,time_stamp))")
    conn.commit()
    for temp_region in eba_regions:
        eia_data = pandas.read_csv(filepath_or_buffer="s3://nima-s3/eia/EBA."+temp_region+"-ALL.H.csv").drop("Unnamed: 0",axis=1)
        eia_data.drop(eia_data.tail(2).index,inplace=True)
        eia_data['timestamp'] = eia_data['timestamp'].apply(lambda ts: pandas.Timestamp(ts))
        year_range = range(eia_data['timestamp'].iloc[-1].year, eia_data['timestamp'].iloc[0].year+1)
        #
        for yr in year_range:
            insert_into_db(eia_data,temp_region,yr)
            conn.commit()
    return None
#
#
###############################################################################
#
#
# Initial run, to do: check if the data and cache tables exist, if yes skip to 
# the update section ... use log instead
#
start_time = str(current_time("s"))
update_time = str(current_time("H"))
#
conn = psycopg2.connect('dbname=main '+psql_settings)
cur = conn.cursor()
#
with open(project_dir+"/logs/log.csv","r") as log_file:
    last_log = log_file.read().strip()
if last_log == '':
    init_db()
else:
    #
    #
    for temp_region in eba_regions:
        main_entries = fetch_main(temp_region)
        window_top = main_entries['time_stamp'].iloc[0]
        window_bottom = main_entries['time_stamp'].iloc[-1]
        #
        recent_entries = fetch_recent(temp_region,window_bottom)
        most_recent = recent_entries['timestamp'].iloc[0]
        #
        if most_recent == window_top:
            pass
        else:
            new_entries = recent_entries[recent_entries['timestamp']>window_top]
            #
            # Check recent_entries against main_entries for updates
            #
            pass
            while window_bottom <= window_top:
                if recent_eq_main(window_bottom):
                    pass
                else:
                    update_row(temp_region,window_bottom)
                    change_history(temp_region,window_bottom)
                window_bottom = window_bottom + timedelta(hours=1)
            insert_into_db(new_entries,temp_region,window_top.year)
            conn.commit()
    #
    #
    end_time = str(current_time("s"))
    # Log:
with open(project_dir+"/logs/log.csv","a") as log_file:
    log_file.write(start_time+", "+end_time+"\n")
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



































