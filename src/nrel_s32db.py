#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
January 2020
@author: nima

This script takes the NRELs TYA3 dataset from an S3 bucket, converts date and time columns in local timezones
to standard pandas timestamps in UTC and averages over the weather stations within each grid region. The result
it stored in a table on our PostgreSQL database.

"""
#############################################################################################################################################
#
#
import os
import boto3
import pandas
import psycopg2 # sqlalchemy is a good alternative
from io import StringIO
from datetime import datetime
from datetime import timedelta
from functools import reduce
#
# Call environment variables
project_dir = os.getenv("project_dir")
#
#
s3bucket = os.getenv("s3_bucket")
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
#############################################################################################################################################
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
#############################################################################################################################################
#
# Define functions to import and reduce the nrel data for each eba_region
#
# stations(reg) outputs the list of file_ids and timezone pairs for the stations 
# situated in the eba region reg
# Assumptions: timezones range from UTC-5 to UTC-9 (Excludes Hawaii UTC-10)
#
def stations(region):
    id_tz_pairs = {}
    for state in eba_regions[region]:
        for entry in nrel_headers[state]:
            id_tz_pairs[entry[0]] = entry[3][0:2]
    return id_tz_pairs
#
#
# Imports station data from file in S3 bucket, returns a dataframe.
#
def from_s3(file_id):
    return pandas.read_csv(StringIO("\n".join(s3.Object(bucket_name=s3bucket[5:], \
            key="nrel/"+file_id+"TYA.CSV").get()['Body'].read().decode("utf-8").split("\r\n")[1:])))
#
#
# Calls from_s3 to import file from S3 and translates the timestamps to pandas standard timestamps, drops extra columns
# and renames the keys.
#
def nrel_data(region,file_id):
    data = from_s3(file_id)[list(nrel_keys.values())].set_axis(['timestamp', 'time', 'ghi_'+file_id, \
                                                                'dni_'+file_id, 'ws_'+file_id], axis='columns', inplace=False)
    data['timestamp'] = (data['timestamp'] + ' ' + data['time'] + stations(region)[file_id]).apply(to_datetime24)
    return data.drop('time',axis=1).set_index('timestamp')
#
#
# to_datetime24 takes a timestamp in the MM/DD/YYYY HH:MM-tz (24:00) format, 
# converts timestamp to pandas datetime and sets the year to year=2010. Returns a timestamp.
#
def to_datetime24(timestamp,year='2010'):
    if timestamp[11:13] != '24':
        timestamp = timestamp[0:6] + year + timestamp[10:]
        return pandas.to_datetime(timestamp).tz_convert(tz=None).replace(year=2010)
    else:
        timestamp = timestamp[0:6] + year + ' 00' + timestamp[13:]
        return (pandas.to_datetime(timestamp).tz_convert(tz=None) + timedelta(days=1)).replace(year=2010)
#
#
# formats a dataframe row to be inseted into a table in the database. Returns a string.
#
def insert_values(region,ts,df_row):
    values = "'{}','{}'".format(region,ts)
    for key in df_row.keys():
        values = values + ',' + "'{}'".format(df_row[key])
    return values
#
#
# Inserts the dataframe rows into the nrel table in the database calling insert_value to format them appropriately.
# Returns None.
#
def insert_into_db(temp_data,temp_region):
    for i in range(len(temp_data)):
        cur.execute("INSERT INTO nrel (region, time_stamp, ghi, dni, wind_speed) VALUES \
                    ({})".format(insert_values(temp_region,temp_data.index[i],temp_data.iloc[i])))
    return None
#
#
#############################################################################################################################################
#
#
start_time = str(current_time("s"))
#
#
conn = psycopg2.connect('dbname=main '+psql_settings)
cur = conn.cursor()
#
#
cur.execute("create table nrel (region varchar(8) not null, time_stamp timestamp not null, ghi real not null, \
            dni real not null, wind_speed real not null, primary key (region, time_stamp))")
for region in eba_regions.keys():
    # Commit data from all the stations in the region to memory
    ghi_data = {}
    dni_data = {}
    ws_data = {}
    st_tz = {}
    region_data = {}
    for st_id in stations(region).keys():
        station_data = nrel_data(region,st_id)
        st_tz = stations(region)[st_id]
        ghi_data[st_id] = station_data['ghi_'+st_id]
        dni_data[st_id] = station_data['dni_'+st_id]
        ws_data[st_id] = station_data['ws_'+st_id]
        del station_data
    # Average GHI, DNI and WS over the stations
    region_data["ghi"] = pandas.DataFrame(reduce(lambda  left,right: pandas.merge(left,right,on=['timestamp']),\
                                                 ghi_data.values()).mean(axis=1)).set_axis(['ghi'],axis='columns',inplace=False)
    region_data["dni"] = pandas.DataFrame(reduce(lambda  left,right: pandas.merge(left,right,on=['timestamp']),\
                                                 dni_data.values()).mean(axis=1)).set_axis(['dni'],axis='columns',inplace=False)
    region_data["ws"] = pandas.DataFrame(reduce(lambda  left,right: pandas.merge(left,right,on=['timestamp']),\
                                                ws_data.values()).mean(axis=1)).set_axis(['wind speed'],axis='columns',inplace=False)
    del ghi_data
    del dni_data
    del ws_data
    # Merge into a single dataset
    region_data = pandas.DataFrame(reduce(lambda  left,right: pandas.merge(left,right,on=['timestamp']), region_data.values())).round(2)
    # Export the result on the database
    insert_into_db(region_data,region)
    conn.commit()
    del region_data
#
end_time = str(current_time("s"))
#
cur.close()
conn.close()
#
#
# Log:
with open(project_dir+"/logs/nrel_logs.csv","a") as log_file:
    log_file.write(start_time+", "+end_time+"\n")
print(end_time)
#
#############################################################################################################################################
#############################################################################################################################################
#############################################################################################################################################
