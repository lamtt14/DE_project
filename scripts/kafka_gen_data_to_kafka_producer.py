from datetime import datetime, timedelta
from kafka import KafkaProducer
import cassandra
import time 
import uuid
import datetime
import math 
import pandas as pd
import numpy as np 
import mysql.connector 
import json 
import random

#initiate kafka producer
kafka_producer = KafkaProducer(bootstrap_servers='broker:29092', value_serializer= lambda x: json.dumps(x).encode('utf-8'))
kafka_topic = "newData_from_gen_source"

#MySQL conf
USER = 'root'
PASSWORD = '1'
HOST = 'mysql'
PORT = '3308'
DB_NAME = 'lamtt14'
URL = 'jdbc:mysql://' + HOST + ':' + PORT + '/' + DB_NAME
DRIVER = "com.mysql.cj.jdbc.Driver"

def retrive_data_from_job():
    connection = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST, database=DB_NAME)
    query = """select id as job_id, campaign_id, group_id, company_id from job"""
    mysql_data = pd.read_sql(query,connection)
    return mysql_data

def retrive_data_from_publisher():
    connection = mysql.connector.connect(user=USER, password=PASSWORD, host=HOST, database=DB_NAME)
    query = """select distinct(id) as publisher_id from master_publisher """
    mysql_data = pd.read_sql(query,connection)
    return mysql_data

def generate_fake_data(n_records):
    publisher = retrive_data_from_publisher()
    publisher = publisher['publisher_id'].to_list()
    jobs_data = retrive_data_from_job()
    job_list = jobs_data['job_id'].to_list()
    campaign_list = jobs_data['campaign_id'].to_list()
    group_list = jobs_data[jobs_data['group_id'].notnull()]['group_id'].astype(int).to_list()
    
    
    i = 0
    while i < n_records:
        create_time = str(cassandra.util.uuid_from_time(datetime.datetime.now()))
        bid = random.randint(0,1)
        interaction =  ['click','conversion','qualified','unqualified']
        custom_track = random.choices(interaction, weights=(70,10,10,10))[0]
        job_id = random.choices(job_list)
        publisher_id = random.choices(publisher) 
        group_id = random.choice(group_list)
        campaign_id = random.choice(campaign_list)
        ts = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        data = {
            "create_time": create_time,
            "bid":bid,
            "campaign_id":campaign_id,
            "custom_track":custom_track,
            "group_id":group_id,
            "job_id":job_id,
            "publisher_id":publisher_id,
            "ts":ts,
        }
        print(data)
        kafka_producer.send(kafka_topic, value=data)
        i+=1 
    return print("Data Generated Successfully")

status = "ON"
while status == "ON":
    generate_fake_data(n_records = random.randint(1,5))
    time.sleep(10)
kafka_producer.close()