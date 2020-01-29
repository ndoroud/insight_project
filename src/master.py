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
import json
import pandas
import psycopg2
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
settings = ""
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
###############################################################################
#
# Define functions to import and reduce the nrel data for each eba_region
#
# stations(r) outputs the list of file_ids for the stations situated in the
# eba region r
def stations(region):
    files = []
    for state in eba_regions[region]:
        for entry in nrel_headers[state]:
            files.append(entry[0])
    return files
#
#
# nrel_append(ld,d) takes a given line, ld, from nrel data files and adds it
# to the dictionary, d
#def nrel_append(line_data,dictionary):
#    year = line_data[0][6:10]
#    month = line_data[0][0:2]
#    day = line_data[0][3:5]
#    if year in dictionary.keys():
#        if month in dictionary[year].keys():
#            if day in dictionary[year][month].keys():
#                dictionary[year][month][day].append(line_data[1:])
#            else:
#                dictionary[year][month][day] = [line_data[1:]]
#        else:
#            dictionary[year][month] = {day : [line_data[1:]]}
#    else:
#        dictionary[year] = {month : {day : [line_data[1:]]}}
#    return None
#
#
# nrel_append(ld,d) takes a given line, ld, from nrel data files and adds it
# to the dictionary, d, stripping the year.
def nrel_append(line_data,dictionary):
    month = line_data[0][0:2]
    day = line_data[0][3:5]
    if month in dictionary.keys():
        if day in dictionary[month].keys():
            dictionary[month][day].append(line_data[1:])
        else:
            dictionary[month][day] = [line_data[1:]]
    else:
        dictionary[month] = {day : [line_data[1:]]}
    return None
#
#
def leap_year_test(data):
    if len(data["02"].keys())==28:
        pass
    else:
        print("leap year")
def count_test(data):
    y = 0
    for month in data.keys():
        y += len(data[month]["01"])
    if y == 12*24:
        pass
    else:
        print("count test failed")
# nrel_data(fid) imports the file associated with the file id, fid, and outputs
# a nested dictionary storing imported data.
def nrel_data(file_id):
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
#
# for DNI, column = 6
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
                
#
#
#
conn = psycopg2.connect(settings[1:])
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
#
#