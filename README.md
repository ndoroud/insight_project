# Grid Insights by Nima Doroud

Developed as part of the Insight's Data Engineering Fellowship 2020A in Boston, MA.

## Table of Contents
1. [Introduction](README.md#Introduction)
1. [Pipeline](README.md#Pipeline)
1. [Setup](README.md#Setup)
1. [Contents](README.md#Contents)

## Introduction

About 75% of the energy used to generate electricity in the US goes to [waste](https://www.edf.org/card/6-ways-cut-big-waste-our-energy-system?card=1) costing roughly $260 billion annualy. Sadly, over 80% of the electricity is generated from non-renewable [sources](https://www.eia.gov/tools/faqs/faq.php?id=427&t=3) taking a massive toll on the environment. With such appaling figures there is plenty of room for improvement, but it requires careful analysis to identify the most viable options. Performing such analysis often involves multiple, diverse datasets. Specifically, for an area to effectively transition to using 100% renewable resources to generate it's electricity it needs to have the right balance of demand and available renewable resources. This information is contained in two separate datasets which need to be ingested and suitably merged.

The present project aims to handle the ingestion and merging of these datasets in an automated fashion to provide the latest data in a database to be used for analysis. It can be deployed on Amazon Web Services (AWS) utilizing EC2 instances for processing and scheduling using python in conjunction with Apache NiFi, S3 storage for storing raw data and PostgreSQL hosted on RDS for merged data. [NREL](https://rredc.nrel.gov/solar/old_data/nsrdb/1991-2005/tmy3/by_USAFN.html)

## Pipeline



## Setup

## Contents
- Deployment
- Application
- Future directions
