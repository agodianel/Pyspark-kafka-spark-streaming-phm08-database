from sqlite3 import Time
from kafka import KafkaProducer
import time
import json
from json import dumps 
from datetime import datetime
from generatore_dati_random import get_utenti_registro
import pandas as pd
import csv


indice=0
#DATA_DIR='/home/xhulio/Scaricati/progetto/predictive-maintenance-spark-master/data/'
#DATA_DIR='/home/xhulio/max_life_train1.csv'
DATA_DIR='/home/xhulio/progetti_kafka/Progetto_motori/max_life_ test.csv'

#train_df= pd.read_csv(DATA_DIR+'train.csv',header=None)
train_df= pd.read_csv(DATA_DIR,header=None)

train_df.columns=['id','cycle','setting1','setting2','setting3','s1','s2','s3','s4','s5','s6','s7','s8','s9','s10','s11','s12','s13','s14','s15','s16','s17','s18','s19','s20','s21','MaxCycle']

def get_partizione(key,all,avalaible):    #due partizioni
    if indice%2==0:
       #print("partizione 0")
       return 0
    else: 
       #print("partizione 1")
       return 0

def json_serialize(data):
    return json.dumps(data).encode('utf-8')



if __name__== "__main__":

    while 1==1:
        my_producer = KafkaProducer(  
        bootstrap_servers = ['localhost:9092'],  
        value_serializer = json_serialize,
        partitioner=get_partizione  
        ) 
        for i in range(0,20630):
                     riga=train_df.loc[i,:].to_json()
                     utenti_registrati=json.loads(riga)
                     indice=indice+1
                     print(utenti_registrati)
                     my_producer.send("univpma",utenti_registrati)
                     time.sleep(0.1)

    
