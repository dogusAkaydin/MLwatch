#! /usr/bin/python3
import os
import sys
import time
import csv
import numpy as np
import json
import config

from kafka import KafkaProducer

def main():
    """Carry-out the main routine, return the wall clock time passed."""
    t0wall = time.time()
    
    urlFilePath = os.path.join(config.MODEL_DIR, 'fall11_urls.txt')

#    KAFKA_TOPIC = 'demo'
#    KAFKA_BROKERS = 'localhost:9092'
 
    KAFKA_TOPIC   = config.KAFKA_CONFIG['topic'] 
    KAFKA_BROKERS = config.KAFKA_CONFIG['brokers'] 
 
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS, 
                             value_serializer=\
                             lambda m: json.dumps(m).encode('UTF-8'))
    

    #producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS) 

    record_number = 1
    with open(urlFilePath, 'r', errors='ignore') as urlFile:
        records = csv.reader(urlFile, delimiter='\t')
        for record in records:
            #print(record_number)
            producer.send(KAFKA_TOPIC, [record_number, record]).get(timeout=1)
            #producer.send(KAFKA_TOPIC, [record_number, record].encode('utf-8')).get(timeout=1)
            record_number += 1
            time.sleep(.1)

        #producer.send(KAFKA_TOPIC, 
        #              key=bytes([line_number]), 
        #              value=url.encode('UTF-8')
        #             ).get(timeout=1)

      
    dtWall = time.time() - t0wall
    return dtWall

if __name__ == '__main__':
    """Command-line execution for producer.py"""
    
    dtWall = main()
    print('DONE in {0:10g} seconds of wall clock time'.format(dtWall))
  
