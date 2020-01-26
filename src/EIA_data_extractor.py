#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 11:14:35 2020

@author: nima
"""
#
#
# Import required libraries for talking to EIA's API and handling json files.
#
# import requests
import json
#
sid = "EBA.CAL-ALL.D.H" # input()
input_data = "/home/nima/Data/project/EIA/"+sid+".json"
data_filename = "/home/nima/Data/project/EIA/"+sid+".csv"
cache_filename = "/home/nima/Data/project/EIA/"+sid+"_cache.csv"
cache_length = 72
#
#
# Load data from a 2 column CSV file.
#
def load_data(input_filename):
    input_file = open(input_filename,"r")
    input_data = []
    for line in input_file:
        # Remove trailing space or /n.
        line_data = line.rstrip().split(",")
        input_data.append(line_data)
    input_file.close()
    return input_data
#
#
# Write/Append (mode=w,a) data into a CSV file.
#
def write_data(data,output_filename,mode):
    output_file = open(output_filename,mode)
    for entry in data:
        output_file.write(entry[0]+","+str(entry[1])+"\n")
    output_file.close()
#
#
# Try loading existing data, set default values if file does not exist.
#
try:
    current_data = load_data(data_filename)
    latest_entry = current_data[-1]
    latest_entry[1] = int(latest_entry[1])
except FileNotFoundError:
    current_data = []
    latest_entry = ["19000101T00Z",0]
#try:
#    current_cache = load_data(cache_filename)
#except FileNotFoundError:
#    pass
#
# Import data from json file
#
with open(input_data) as json_file:
    data = json.load(json_file)["series"][0]["data"]
    while data[0][0][0:8]+data[0][0][9:11] < latest_entry[0][0:8]+latest_entry[0][9:11]:
            data.pop(0);
#
#
l = len(data) - cache_length
data.reverse()
#
#
if l > 0:
    write_data(data[0:l],data_filename,"a")
    write_data(data[l:],cache_filename,"w")
elif l <= 0:
    write_data(data,cache_filename,"w")


