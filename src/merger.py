#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 22 17:43:39 2020

@author: nima

Notes: Pandas reading json directly from S3 requires s3fs.
"""
#
#
# Import packages
#
import boto3
import json
import pandas
import psycopg2
#
#
# Setup access parameters for the data origin (S3)
#
s3 = boto3.resource('s3')
#
# for bucket in s3.buckets.all():
#     print(bucket.name)
#
census_data_s3 = s3.Object(bucket_name='nima-s3', key="CAL_2010-2018.csv")
eia_data_s3 = s3.Object(bucket_name='nima-s3', key="EBA.CAL-ALL.D.H.json")
#
#
# Setup access to the destination
#
with open("/home/nima/Data/project/keys/psql.json","r") as file:
    psql_settings = json.load(file)
settings = ""
for key in psql_settings.keys():
    settings = settings +" "+ key + "=" + psql_settings[key]
#
#
# response = obj.get()
# data = response["Body"].read()
#
#import json
#import io
#census_data = pandas.read_csv(io.BytesIO(census_data_s3.get()['Body'].read()))
#eia_data = json.loads(eia_data_s3.get()["Body"].read().decode("utf-8"))
#
#
#
#
census_data = pandas.read_csv(filepath_or_buffer="s3://nima-s3/CAL_2010-2018.csv")
eia_data = pandas.read_json(path_or_buf="s3://nima-s3/EBA.CAL-ALL.D.H.json",lines=True)
#
#
population_estimate = census_data.iloc[0].to_dict()
population_estimate['2019']=population_estimate['2018']
population_estimate['2020']=population_estimate['2018']
hourly_data = eia_data.iloc[0][1][0]["data"]
#
#
conn = psycopg2.connect(settings[1:])
cur = conn.cursor()
cur.execute("CREATE TABLE data (date date not null, population int not null, hour time not null, demand int not null)")
for entry in hourly_data:
    d = entry[0][0:4]+"-"+entry[0][4:6]+"-"+entry[0][6:8]
    p = population_estimate[entry[0][0:4]]
    h = entry[0][9:11]+":00:00"
    c = entry[1]
    cur.execute("INSERT INTO data (date, population, hour, demand) VALUES ('{}','{}','{}','{}')".format(d,p,h,c))
    conn.commit()
cur.close()
conn.close()

#
#
# Create a nested dictionary with the following format:
# {year(yyyy) : {"Population" : pop , month(mm): [hourly_usage]}}
#months = ["0{}".format(mm)[-2:] for mm in range(1,13)]
#def merge(data,population):
#    report = {}
#    for item in data:
#        year = item[0][0:4]
#        month = item[0][4:6]
#        if year in report.keys():
#            report[year][month].append(item)
#        else:
#            report[year] = {"Population" : population[year]}
#            for mm in months:
#                report[year][mm] = []
#            report[year][month].append(item)
#    return report
#
#
# data = pandas.read_sql("select * from names", conn)
#
#
#def monthly_total(year,month):
#    x = pandas.DataFrame(data[year][month]).sum(axis = 0).iloc[1]
#    return x
#
#
#def yearly_total(year):
#    x = sum([monthly_total(year,mm) for mm in months])
#    return x
#
#
#def per_capita(year,value):
#    x = value/data[year]["Population"]
#    return x
#
#
#query = input("Electricity consumption per capita for the year: ")
#
#print("is {} MWh.".format(per_capita(str(query),yearly_total(str(query)))))
#
#
#
#