from asyncio.subprocess import Process
from concurrent.futures import process, thread
from distutils.command.build_scripts import first_line_re
import imp
from inspect import _void
from itertools import cycle
from lib2to3.pgen2.pgen import DFAState
from operator import index
from random import betavariate
from re import A
import math
from select import select
from sre_constants import FAILURE
from tkinter import EXCEPTION, W
from unicodedata import east_asian_width
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.types import *
from pyspark.sql.functions import *
from struttura import jsonparse
import pyspark.pandas as ps
import time
import matplotlib.pyplot as plt
import pandas as pd
from pyspark.sql.types import DoubleType
import json
from pyspark.sql.types import IntegerType,BooleanType,DateType
from pyspark.ml import Estimator
import os
from pyspark.sql import functions as F
from pyspark.sql.functions import lit,unix_timestamp
import numpy as np
import seaborn as sns
import matplotlib.pyplot as plt
from ast import Index
from cProfile import label
from dataclasses import dataclass
from itertools import groupby
import string
from turtle import color, shape
from scipy import stats
from scipy.stats import exponweib
from scipy.optimize import fmin
import time
from threading import Thread
from reliability.Fitters import Fit_Weibull_2P,Fit_Weibull_3P
import matplotlib.pyplot as plt
from reliability.Probability_plotting import plot_points
from kafka import KafkaProducer

#per avviare eseguire il comando sotto dopo $
#hadoop@xhulio:/usr/local/spark/bin$ spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0 kafka_structured_con_memoria.py

pd.set_option('display.max_rows',None)
DIR="/Hadoop_File/max_life_train1.csv"





###############################
# UDF FUNCTIONS
##############################

def get_log(u):
    a=math.log(u)
    return a

log_udf = udf(lambda z: get_log(z), FloatType())

def get_varinza(n,ln,media):
  try:  
    varianza=1/((n-1)*(ln-media)**2)
    return varianza
  except:  
    return 0

prendi_varianza = udf(lambda z,x,y: get_varinza(z,x,y), FloatType())

def get_beta(devst):
  try:  
    beta=np.pi/(devst*((6)**(1/2)))
    return beta
  except:  
    return 0

prendi_beta = udf(lambda z: get_beta(z), FloatType())

def get_eta(media,beta):
  try:  
    eta=media+(0.5772/beta)
    return eta
  except:  
    return 0

prendi_eta = udf(lambda z,y: get_eta(z,y), FloatType())


def get_prob(ciclo,n_scala,n_shapee):  #CDF
  try:
    prob=1-(math.exp(-(ciclo/n_scala)**n_shapee))
    return prob
  except:  
    return 0

#p_failure = udf(lambda z: get_prob(z,avg_ln_weib.select('eta'),avg_ln_weib.select('beta')), FloatType())
p_failure = udf(lambda z,x,y: get_prob(z,x,y) , FloatType())



def get_prob3p(ciclo,n_scala,n_shapee,gamma):     #CDF
  try:
    prob=1-math.exp(-((ciclo-gamma)/n_scala))**n_shapee
    return prob
  except:  
    return 0

#p_failure = udf(lambda z: get_prob(z,avg_ln_weib.select('eta'),avg_ln_weib.select('beta')), FloatType())
p_failure3p = udf(lambda z,x,y,t: get_prob3p(z,x,y,t) , FloatType())


def get_pdf_prob3p(ciclo,n_scala,n_shapee,gamma):     #CDF
  try:
    r=1-math.exp(-((ciclo-gamma)/n_scala)**n_shapee)
    prob=(n_shapee/(n_scala**n_shapee))*((ciclo-gamma)**(n_shapee-1))*r
    return prob
  except:  
    return 0

#p_failure = udf(lambda z: get_prob(z,avg_ln_weib.select('eta'),avg_ln_weib.select('beta')), FloatType())
pdf_failure3p = udf(lambda z,x,y,t: get_pdf_prob3p(z,x,y,t) , FloatType())


def get_mttf(col1,col2):
  try:  
      mttf=col1*(math.gamma(1+1/col2))
      return int(mttf)
  except:  
    return 0

mttf = udf(lambda z,x: get_mttf(z,x) , IntegerType())

def get_mttf3p(col1,col2,col3):
  try:  
      mttf=col3+col1*(math.gamma(1+1/col2))
      return int(mttf)
  except:  
    return 0

mttf3p = udf(lambda z,x,t: get_mttf3p(z,x,t) , IntegerType())


###############################
# TRAINING
##############################

bootstrapServers = "localhost:9092"
subscribeType = "subscribe"
topics = "univpma"
topic_sender="alert"
producer = KafkaProducer(bootstrap_servers=bootstrapServers)

spark = SparkSession\
        .builder\
        .appName("StructuredKafkaWordCount")\
        .master("local[*]")\
        .config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0")\
        .config("spark.jars.packages","graphframes:graphframes:0.8.2-spark3.0-s_2.12")\
        .config("spark.jars.repositories", "https://repos.spark-packages.org")\
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled","false")\
        .getOrCreate()
colonne=['id','cycle','setting1','setting2','setting3','s1','s2','s3','s4','s5','s6','s7','s8','s9','s10','s11','s12','s13','s14','s15','s16','s17','s18','s19','s20','s21','MaxCycle']
schema=StructType()\
  .add("id",IntegerType(),True)\
  .add("cycle",IntegerType(),True)\
  .add("setting1",FloatType(),True)\
  .add("setting2",FloatType(),True)\
  .add("setting3",FloatType(),True)\
  .add("s1",FloatType(),True)\
  .add("s2",FloatType(),True)\
  .add("s3",FloatType(),True)\
  .add("s4",FloatType(),True)\
  .add("s5",FloatType(),True)\
  .add("s6",FloatType(),True)\
  .add("s7",FloatType(),True)\
  .add("s8",FloatType(),True)\
  .add("s9",FloatType(),True)\
  .add("s10",FloatType(),True)\
  .add("s11",FloatType(),True)\
  .add("s12",FloatType(),True)\
  .add("s13",FloatType(),True)\
  .add("s14",FloatType(),True)\
  .add("s15",FloatType(),True)\
  .add("s16",FloatType(),True)\
  .add("s17",FloatType(),True)\
  .add("s18",FloatType(),True)\
  .add("s19",FloatType(),True)\
  .add("s20",FloatType(),True)\
  .add("s21",FloatType(),True)\
  .add("MaxCycle",IntegerType(),True)

spark.sparkContext.setLogLevel("ERROR")
df = spark.read.format("csv").option("header",False).schema(schema).load(DIR)
df_filtered=df.select('id','cycle','MaxCycle') # selezione delle colonne necessarie per anaisi
df_filtered=df_filtered.withColumn("RUL",col('MaxCycle')-col('cycle')) # calcolo del RUL
df_failure=df_filtered.where(col('RUL')==0).select('id','cycle','RUL') #  inizio filtraggio per RUL
failure_dataset=df_failure.select('cycle')
list_failure=failure_dataset.toPandas().values.reshape(-1)
try: wb=Fit_Weibull_2P(failures=list_failure,show_probability_plot=False,print_results=False)
except: pass
# df_final=df_filtered.withColumn("scala",lit(wb.alpha))
# df_final=df_final.withColumn("shape",lit(wb.beta))
# df_final=df_final.withColumn("P_failure",p_failure(col('cycle'),col('scala'),col('shape')))
df_final=df_filtered.withColumn("scala",lit(wb.alpha))
df_final=df_final.withColumn("shape",lit(wb.beta))
#df_final=df_final.withColumn("gamma",lit(wb.gamma))
#df_final=df_final.withColumn("P_failure",p_failure3p(col('cycle'),col('scala'),col('shape'),col('gamma')))
df_final=df_final.withColumn("P_failure2p",p_failure(col('cycle'),col('scala'),col('shape')))
df_final.sort(F.desc('id'),F.desc('cycle')).show() #visualizza tabella di OUTPUT dal piu recente
df_final.toPandas().to_csv('/home/hadoop/dati_filtrati_intero_prova_f3p.csv',header=True,index=False)
df_final.show()



# ###############################
# # STREAMING
# ##############################

lines = spark\
        .readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", bootstrapServers)\
        .option("header",True)\
        .option(subscribeType, topics)\
        .option("startingOffsets", "earliest")\
        .load()
   
linesbis=lines\
        .selectExpr("CAST(value AS STRING) as json")\
        .withColumn("id", jsonparse(col("json"), lit("id")))\
        .withColumn("cycle", jsonparse(col("json"), lit("cycle")))\
        .withColumn("setting1", jsonparse(col("json"), lit("setting1")))\
        .withColumn("setting2", jsonparse(col("json"), lit("setting2")))\
        .withColumn("setting3", jsonparse(col("json"), lit("setting3")))\
        .withColumn("s1", jsonparse(col("json"), lit("s1")))\
        .withColumn("s2", jsonparse(col("json"), lit("s2")))\
        .withColumn("s3", jsonparse(col("json"), lit("s3")))\
        .withColumn("s4", jsonparse(col("json"), lit("s4")))\
        .withColumn("s5", jsonparse(col("json"), lit("s5")))\
        .withColumn("s6", jsonparse(col("json"), lit("s6")))\
        .withColumn("s7", jsonparse(col("json"), lit("s7")))\
        .withColumn("s8", jsonparse(col("json"), lit("s8")))\
        .withColumn("s9", jsonparse(col("json"), lit("s9")))\
        .withColumn("s10", jsonparse(col("json"), lit("s10")))\
        .withColumn("s11", jsonparse(col("json"), lit("s11")))\
        .withColumn("s12", jsonparse(col("json"), lit("s12")))\
        .withColumn("s13", jsonparse(col("json"), lit("s13")))\
        .withColumn("s14", jsonparse(col("json"), lit("s14")))\
        .withColumn("s15", jsonparse(col("json"), lit("s15")))\
        .withColumn("s16", jsonparse(col("json"), lit("s16")))\
        .withColumn("s17", jsonparse(col("json"), lit("s17")))\
        .withColumn("s18", jsonparse(col("json"), lit("s18")))\
        .withColumn("s19", jsonparse(col("json"), lit("s19")))\
        .withColumn("s20", jsonparse(col("json"), lit("s20")))\
        .withColumn("s21", jsonparse(col("json"), lit("s21")))\
        .withColumn("MaxCycle", jsonparse(col("json"), lit("MaxCycle")))\
        .select("id","cycle","setting1","setting2","setting3","s1","s2","s3","s4","s5","s6","s7","s8","s9","s10","s10","s12","s13","s14","s15","s16","s17","s18","s19","s20","s21","MaxCycle") 


new_df=linesbis.select('id','cycle','MaxCycle') # selezione delle colonne necessarie per anaisi
new_df=new_df.withColumn('timestamp',current_timestamp()) # ho messo il timestamp per fare le prove con la funzione window e watermark
new_df=new_df.withColumn("RUL",col('MaxCycle')-col('cycle')) # calcolo del RUL
df_failure=new_df.where(col('RUL')==0).select('timestamp','id','cycle','RUL') #  inizio filtraggio per RUL
stream_failure_dataset=df_failure.select('cycle')
df_data_coming=linesbis.select('id','cycle','MaxCycle') # selezione delle colonne necessarie per anaisi



query1= stream_failure_dataset\
         .writeStream\
         .outputMode("append")\
         .queryName("Tab_coefficenti")\
         .format("memory")\
         .trigger(processingTime='1 seconds')\
         .start()  

query2= df_data_coming\
         .writeStream\
         .outputMode("append")\
         .queryName("Tab_dati_input")\
         .format("memory")\
         .trigger(processingTime='1 seconds')\
         .start() 

# query3 = lines\
#          .writeStream\
#          .outputMode("append")\
#          .format("console")\
#          .trigger(processingTime='1 seconds')\
#          .start()

# query6 = linesbis\
#          .writeStream\
#          .outputMode("append")\
#          .format("console")\
#          .trigger(processingTime='1 seconds')\
#          .start()

# query4 = stream_failure_dataset\
#          .writeStream\
#          .outputMode("append")\
#          .format("console")\
#          .trigger(processingTime='1 seconds')\
#          .start()

# query5 = df_data_coming\
#          .writeStream\
#          .outputMode("append")\
#          .format("console")\
#          .trigger(processingTime='1 seconds')\
#          .start()

indice_sub=0
stampa_kafka=0


# # ###############################
# # Near real time analisy
# # ##############################


while(1==1):
   indici_failure_memoria=spark.sql("select * from Tab_coefficenti")
   dati_input=spark.sql("select * from Tab_dati_input")
   list_indici_failure_memoria=indici_failure_memoria.toPandas().values.reshape(-1)
   list_dati_input=dati_input.select('id').toPandas().values.reshape(-1)
  # indice=len(list_indici_failure_memoria)
   indice=len(list_dati_input)
   if(indice>0):
     whole_failure=np.append(list_failure,list_indici_failure_memoria) #aggregazione delle dati nuovi, dati vecchi su un unica lista
     if(indice>indice_sub):
     #if(indice>0):    
      indice_sub=indice
      try: 
           stream_wb=Fit_Weibull_3P(failures=whole_failure,show_probability_plot=False,print_results=True)
           stream_wb2p=Fit_Weibull_2P(failures=whole_failure,show_probability_plot=False,print_results=False)
      except: print("not fitting")
      my_df=pd.DataFrame(whole_failure)
      #my_df.to_csv('/home/hadoop/array.csv',header=False,index=False) # salvo il file in locale per fare i grafici
      sparkDF=spark.createDataFrame(my_df)
      sparkDF.write.mode("overwrite").csv("hdfs://xhulio:9000/Hadoop_File/array_failure1")  # PER SCRIVERE I FILE SU HADOOP

      dati_input=dati_input.withColumn('3p_scala',lit(stream_wb.alpha))
      dati_input=dati_input.withColumn('3p_shape',lit(stream_wb.beta))
      dati_input=dati_input.withColumn('3p_gamma',lit(stream_wb.gamma)) # con 3 paramentre
      dati_input=dati_input.withColumn('2p_scala',lit(stream_wb2p.alpha))
      dati_input=dati_input.withColumn('2p_shape',lit(stream_wb2p.beta))
    
      dati_input=dati_input.withColumn("2P_failure",p_failure(col('cycle'),col('2p_scala'),col('2p_shape')))
      dati_input=dati_input.withColumn("3P_failure",p_failure3p(col('cycle'),col('3p_scala'),col('3p_shape'),col('3p_gamma'))) # con 3 paramentre
      #dati_input=dati_input.withColumn("Pdf",pdf_failure3p(col('cycle'),col('scala'),col('shape'),col('gamma'))) # con 3 paramentre
      dati_input=dati_input.withColumn("2p_RUL_STIMATO",mttf(col('2p_scala'),col('2p_shape'))-col('cycle'))
      dati_input=dati_input.withColumn("3p_RUL_STIMATO",mttf3p(col('3p_scala'),col('3p_shape'),col('3p_gamma'))-col('cycle')) # con 3 parametri
      dati_input=dati_input.withColumn("stato",when(col('3P_failure')>0.7,"Warning").otherwise("OK"))
      dati_input=dati_input.withColumn("2p_errore",(abs(mttf(col('2p_scala'),col('2p_shape'))-col('MaxCycle'))/col('MaxCycle')))
      dati_input=dati_input.withColumn("3p_errore",(abs(mttf3p(col('3p_scala'),col('3p_shape'),col('3p_gamma'))-col('MaxCycle'))/col('MaxCycle')))
      dati_input_filtrati=dati_input.select('id','cycle','2P_failure','3P_failure','2p_RUL_STIMATO','3p_RUL_STIMATO','stato','2p_errore','3p_errore')

      kafka_list=dati_input.select('id').toPandas().values.reshape(-1)
      kafka_list2=dati_input.select('3P_failure').toPandas().values.reshape(-1)
      print(len(kafka_list))
      print(len(kafka_list2))
      for i in range(0,len(kafka_list)):
        if((kafka_list2[i]>0.7)and(kafka_list[i]>stampa_kafka)):
          stampa_kafka=kafka_list[i]
          messaggio="Motore id: "+str(kafka_list[i])+" da controllare, probabilita di rottura al: "+str(kafka_list2[i]*100)+"%"
          producer.send(topic_sender,messaggio.encode('utf-8'))
          producer.flush()
      
      dati_input_filtrati.sort(F.desc('id'),F.desc('cycle')).show() #visualizza tabella di OUTPUT dal piu recente
      dati_input_filtrati.toPandas().to_csv('/home/hadoop/dati_filtrati_intero.csv',header=True,index=False)
      media3p=dati_input_filtrati.agg(avg('3p_errore'))
      media2p=dati_input_filtrati.agg(avg('2p_errore'))
     # media2p.show()
     #media3p.show()

  
query1.awaitTermination()