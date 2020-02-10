# Grid Insights by Nima Doroud

Developed as part of the Insight's Data Engineering Fellowship 2020A in Boston, MA.

## Table of Contents
1. [Introduction](README.md#Introduction)
1. [Pipeline](README.md#Pipeline)
1. [Setup](README.md#Setup)
1. [Contents](README.md#Contents)

## Introduction

About 75% of the energy used to generate electricity in the US goes to [waste](https://www.edf.org/card/6-ways-cut-big-waste-our-energy-system?card=1) costing roughly $260 billion annually. Sadly, over 80% of the electricity is generated from non-renewable [sources](https://www.eia.gov/tools/faqs/faq.php?id=427&t=3) taking a massive toll on the environment. With such appalling figures there is plenty of room for improvement, but it requires careful analysis to identify the most viable options. Performing such analysis often involves multiple, diverse datasets. Specifically, for an area to effectively transition to using 100% renewable resources to generate its electricity it needs to have the right balance of demand and available renewable resources. This information is contained in two separate datasets which need to be ingested and suitably merged.

The present project aims to handle the ingestion and merging of these datasets at regular intervals in an automated fashion to provide the latest data in a database to be used for analysis. It can be deployed on Amazon Web Services (AWS) utilizing EC2 instances for processing and scheduling using python in conjunction with Apache NiFi, S3 storage for storing the raw data and PostgreSQL hosted on RDS for merged data.

## Pipeline

![Pipeline](docs/pipeline.png)

The data is ingested and stored in an S3 bucket. It is then accessed by a script from an EC2 instance and merged in a suitable fashion before being written in a PostgreSQL database powered by RDS. The workflow is managed by Apache NiFi and runs at regular intervals. The dash app can be hosted on an EC2 instance to privide access to the database.

## Setup

The pipeline requires EC2 instances, S3 storage and a PostgreSQL database. The python code requires python3 and imports the following packages: os, io, requests, boto3, psycopg2, sqlalchemy, pandas, functools and datetime. awscli and s3fs are also required. Global variables such as credentials and api keys are imported as environment variables.



The data is ingested from NREL and through an API from EIA. The [NREL](https://rredc.nrel.gov/solar/old_data/nsrdb/1991-2005/tmy3/by_USAFN.html) data -- CSV files listed in the "lists/nrel_filelist.csv" -- are assumed to be in an S3 bucket. These are read by "/src/nrel_s32db.py" running on an EC2 instance which averages the data from all the stations within each region and stores the result in a table in the database.




## Contents
- Deployment
- Application
- Future directions
