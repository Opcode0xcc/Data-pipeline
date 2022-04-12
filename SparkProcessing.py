from pyspark.sql import SparkSession

from pyspark.sql.functions import *
from pyspark.sql.types import StringType, StructField, StructType, IntegerType, DoubleType
import os
import pandas as pd
from pyspark.sql import functions as F, types as T
import os
from kafka import KafkaConsumer, consumer
import csv
import kafka

import warnings
warnings.filterwarnings('ignore')
import plotly.express as px
import plotly.graph_objects as go
import plotly.figure_factory as ff
from plotly.subplots import make_subplots
import plotly as py
py.offline.init_notebook_mode(connected = True)

import matplotlib.pyplot as plt
%matplotlib inline

import folium

import math
import random
from datetime import timedelta


spark = SparkSession.builder.appName('Covid data Analysis').getOrCreate()


    
df_raw = (spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "0.0.0.0:9092") # kafka server
  .option("subscribe", "Covid19") # topic
  .option("startingOffsets", "earliest") # start from beginning 
  .load())


df_json = df_raw.selectExpr('CAST(value AS STRING) as json')

def analysis(data):
    import requests
    import json
    
    result = requests.post('http://localhost:9000/predict', json=json.loads(data))
    return json.dumps(result.json())
vader_udf = udf(lambda data: analysis(data), StringType())


schema_input = StructType([StructField('data', StringType())])
Covid_schema = StructType([\
                           StructField("FIPS", IntegerType()),\
                           StructField("Admin2", StringType()),\
                           StructField("Province_State", StringType()),\
                           StructField("Country_Region", StringType()),\
                           StructField("Last_Update", StringType()),\
                           StructField("Lat", DoubleType()),\
                           StructField("Long", DoubleType()),\
                           StructField("Confirmed", IntegerType()),\
                           StructField("Deaths", IntegerType()),\
                           StructField("Recovered", IntegerType()),\
                           StructField("Active", IntegerType()),\
                           StructField("Combined_Key", StringType()),\
                           StructField("Incident_Rate", DoubleType()),\
                           StructField("Case_Fatality_Ratio", DoubleType())])



df = df_json.select(from_json(df_json.json, schema_input).alias('records'),\
               from_json(vader_udf(df_json.json), Covid_schema).alias('responce'))\
.select('records','responce.*')\
.writeStream \
.outputMode("complete") \
.trigger(once=True) \
.format("console") \
.start() \
.awaitTermination()


df.show()


df.printSchema()
print((df.count(), len(df.columns)))
df.describe().show()

#Data Cleaning & Preparation

for col in df.columns:
  print(col + ":" , df[df[col].isNull()].count())

# Removing FIPS column because its all null data and can impact on overall performance
df = df.drop('FIPS')

# Mannually Saperating Null columns dataframe so we don't loss data

Admin2_Null_Data = df[df['Admin2'].isNull()] # Admin2 isNull
Admin2_NotNull_Data = df[df['Admin2'].isNotNull()] # Admin2 isNotNull
Province_State_Null_Data = Admin2_NotNull_Data[Admin2_NotNull_Data['Province_State'].isNull()]  # Province_State isNull 
Province_State_NotNull_Data = Admin2_NotNull_Data[Admin2_NotNull_Data['Province_State'].isNotNull()] # Province_State isNotNull
Lat_Null_Data = Province_State_NotNull_Data[Province_State_NotNull_Data['Lat'].isNull()]  # Lat isNull 
Lat_NotNull_Data = Province_State_NotNull_Data[Province_State_NotNull_Data['Lat'].isNotNull()] # Lat isNotNull
Long_Null_Data = Lat_NotNull_Data[Lat_NotNull_Data['Long'].isNull()]  # Long isNull 
Long_NotNull_Data = Lat_NotNull_Data[Lat_NotNull_Data['Long'].isNotNull()] # Long isNotNull
Incident_Rate_Null_Data = Long_NotNull_Data[Long_NotNull_Data['Incident_Rate'].isNull()]  # Incident_Rate isNull 
Incident_Rate_NotNull_Data = Long_NotNull_Data[Long_NotNull_Data['Incident_Rate'].isNotNull()] # Incident_Rate isNotNull
Case_Fatality_Ratio_Null_Data = Incident_Rate_NotNull_Data[Incident_Rate_NotNull_Data['Case_Fatality_Ratio'].isNull()]  # Case_Fatality_Ratio isNull 
Case_Fatality_Ratio_NotNull_Data = Incident_Rate_NotNull_Data[Incident_Rate_NotNull_Data['Case_Fatality_Ratio'].isNotNull()] #Case_Fatality_Ratio isNotNull

Deaths_Null_Data = Case_Fatality_Ratio_NotNull_Data[Case_Fatality_Ratio_NotNull_Data['Deaths'].isNull()] # Deaths isNull
Deaths_NotNull_Data = Case_Fatality_Ratio_NotNull_Data[Case_Fatality_Ratio_NotNull_Data['Deaths'].isNotNull()] 
Recovered_Null_Data = Deaths_NotNull_Data[Deaths_NotNull_Data['Recovered'].isNull()] # Recovered isNull
Recovered_NotNull_Data = Deaths_NotNull_Data[Deaths_NotNull_Data['Recovered'].isNotNull()] # Recovered isNotNull
Active_Null_Data = Recovered_NotNull_Data[Recovered_NotNull_Data['Active'].isNull()] # Active isNull
Final_DF = Recovered_NotNull_Data[Recovered_NotNull_Data['Active'].isNotNull()] # Active isNotNull

Final_DF.count()

# Verifing again for null data

for col in Final_DF.columns:
  print(col + ":" , Final_DF[Final_DF[col].isNull()].count())


# Checking for data which has value 0 on it

def count_zeros():
    columns_list = ['Admin2', 'Province_State', 'Country_Region', 'Last_Update', 'Lat', 'Long', 'Confirmed', 'Deaths', 'Recovered', 'Active', 'Combined_Key', 'Incident_Rate', 'Case_Fatality_Ratio']
    for col in columns_list:
        print(col + ':', Final_DF[Final_DF[col]==0].count() )
count_zeros()
    

Final_DF.show()

# As we don't have records for last 14 days to the current_date so i am coming up with last 14days from the latest record we have

base = datetime.date(2021,4,2)
date_upto_14days = base - datetime.timedelta(days=14)
print(date_upto_14days)
Last_14Days = Final_DF.filter(Final_DF.Last_Update >= date_upto_14days)


# Top10_country = Last_14Days.orderBy(col(Active).asc()).distinct().take(10)
Top10_country = Last_14Days.orderBy(asc("Active")).distinct().take(10)
#print(Top10_country)

# Top 10 countries with decreasing positive cases in the last 14 days.

 
for i in Top10_country:
    #print(f"Country : %4d, Active Cases: %3d, Admin2 : %2d, Province_State :%1d  "  %(i[2] , i[9], i[0], i[1]))
    print("Country :{} decreasing positive Cases:{}, Admin2 : {}, Province_State : {}  ".format(i[2] , i[9], i[0], i[1]))

  

# Top 3 provinces/states in those countries with the highest positive cases during the period


Top3_Highest_Positive_cases = Last_14Days.orderBy(col("Active").desc()).distinct().take(3)

for i in Top3_Highest_Positive_cases[0:3]:
    print("Admin2 : {}, Province/state: {}, Highest Positive Cases {}, Lat : {},Long : {}".format(i[0], i[1], i[9], i[4], i[5]))
    
    
    
    
    
temp = pd.DataFrame(Top3_Highest_Positive_cases)
m = folium.Map(location=[0,0], tiles='cartodbpositron', min_zoom = 1, max_zoom=4, zoom_start=1)

for i in range(0, len(temp)):
    folium.Circle(location=[temp.iloc[i][4], temp.iloc[i][5]], color = ' crimson', fill = 'crimson',
                 tooltip = '<li><bold> Country: '  + str(temp.iloc[i][2])+
                  '<li><bold>  Province/state: '  + str(temp.iloc[i][1])+
                  '<li><bold> Active: '  + str(temp.iloc[i][9])+
                  '<li><bold> Deaths: '  + str(temp.iloc[i][7]),
                  radius = int(temp.iloc[i][6])**0.5).add_to(m)
