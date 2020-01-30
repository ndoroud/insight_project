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
import psycopg2
import json
import pandas
import datetime
from io import StringIO
from functools import reduce
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
#eia_data_s3 = s3.Object(bucket_name='nima-s3', key="")
#
#
###############################################################################
#
# Import lists as dictionaries
#
states = {}
with open(project_folder+"/lists/list_of_states.csv","r") as listofstates:
    for line in listofstates:
        state = line.strip().split(",")
        states[state[0]] = state[1]
#
#
eba_regions = {}
with open(project_folder+"/lists/eba_regions.csv","r") as ebaregions:
    for line in ebaregions:
        region = line.strip().split(",")
        eba_regions[region[0]] = region[1:]
#
#
nrel_headers = {}
with open(project_folder+"/lists/nrel_headers.csv","r") as nrelheaders:
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
             "dni": 'DNI (W/m^2)'}
             #"wind": 'Wspd (m/s)'}
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
    cur.execute("CREATE TABLE nrel_{} (date date not null, hour time not null, DNI decimal not null)".format(region))
    for st in stations("CAR"):
        region_data.append(dni_data(st))
    region_data = reduce(lambda  left,right: pandas.merge(left,right,on=['timestamp']), region_data)
    region_data = region_data.mean(axis=1)
    for i in region_data.index:
        cur.execute("INSERT INTO nrel_{} (date, hour, dni) VALUES \
                    ('{}','{}','{}')".format(region,region_data[[i]].keys()[0],int(region_data[i])))
    conn.commit()
    del region_data
cur.close()
conn.close()

                

"""

data.set_axis(['timestamp', region+"_"+ser], axis='columns', inplace=False)
reduce(lambda left,right: pandas.merge(left,right,how='outer',on='timestamp'), temp_data)
def nrel_data(file_id):
    data = pandas.read_csv(from_s3(file_id))

    data={}
    #   For openening local files
    #with open(project_folder+"/data/nrel/"+file_id+"TYA.CSV","r") as file:
    #   For accessig files on S3
    file = s3.Object(bucket_name='nima-s3', key="nrel/"+file_id+"TYA.CSV").get()['Body']
    file = file.read().decode("utf-8").split("\r\n")
    for line in file:
        line_data = line.strip().split(",")
        if line_data[0] in [file_id,"Date (MM/DD/YYYY)",""]:
            pass
        #line_data[0] = "MM/DD/YYYY"
        #nrel_append(line_data,data)
        else:
            nrel_append(line_data,data)
    leap_year_test(data)
    count_test(data)
    return data
#
#
def nrel_avg(region,column):
    data = {}
    nos = len(stations(region))
    for file_id in stations(region):
        temp_data = nrel_data(file_id)
        for month in temp_data.keys():
            if month not in data.keys():
                data[month] = {}
            for day in temp_data[month].keys():
                if day in data[month].keys():
                    for i in range(24):
                        data[month][day][i][1] += float(temp_data[month][day][i][column])/nos
                else:
                    data[month][day] = [[temp_data[month][day][i][0],float(temp_data[month][day][i][column])/nos] for i in range(24)]
    return data


conn = psycopg2.connect(settings)
cur = conn.cursor()
for region in eba_regions.keys():
    cur.execute("CREATE TABLE nrel_{} (date date not null, hour time not null, DNI decimal not null)".format(region))
    data = nrel_avg(region,6)
    for month in data.keys():
        for day in data[month].keys():
            d = "2000-"+month+"-"+day
            for entry in data[month][day]:
                h = entry[0]+":00"
                dni = entry[1]
                cur.execute("INSERT INTO nrel_{} (date, hour, dni) VALUES ('{}','{}','{}')".format(region,d,h,dni))
                conn.commit()
cur.close()
conn.close()
#df['time'] = df['time'].apply(my_to_datetime)
#
#
file = s3.Object(bucket_name='nima-s3', key="nrel/690150TYA.CSV").get()['Body']
file = StringIO("\n".join(file.read().decode("utf-8").split("\r\n")[1:]))
file = pandas.read_csv(file)
#[["Date (MM/DD/YYYY)","Time (HH:MM)",'DNI (W/m^2)']]
#
"""

