
import os
import glob
import pandas as pd
from kafka import KafkaProducer
import logging
from json import dumps, loads
import csv
logging.basicConfig(level=logging.INFO)


producer = KafkaProducer(bootstrap_servers='127.0.0.1:9092', value_serializer=lambda 
K:dumps(K).encode('utf-8'))


os.chdir("/home/jovyan/csse_covid_19_daily_reports/")
extension = 'csv'
all_filenames = [i for i in glob.glob('*.{}'.format(extension))]
#combine all files in the list
combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])
#export to csv
combined_csv.to_csv( "Merged.csv", index=False, encoding='utf-8-sig')


with open("/home/jovyan/csse_covid_19_daily_reports/Merged.csv" , 'r' ,encoding = 'utf-8' ) as file:
   reader = csv.reader(file, delimiter = ',')
   for messages in reader:
       producer.send('Covid19', messages)
       producer.flush()




