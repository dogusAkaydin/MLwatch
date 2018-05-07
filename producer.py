import os
import sys
import time
import csv
import numpy as np
import json
#from urllib.request import Request, urlopen
#from urllib.error import  URLError
#import socket
#import errno

from kafka import KafkaProducer

# TF-related imports
# import tensorflow as tf
# from datasets import imagenet
# from nets import inception
# from preprocessing import inception_preprocessing

#slim = tf.contrib.slim

#image_size = inception.inception_v1.default_image_size

def main(urlFilePath = './url.txt'):
    """Carry-out the main routine, return the wall clock time passed."""
    t0wall = time.time()

    KAFKA_TOPIC = 'demo'
    KAFKA_BROKERS = 'localhost:9092'
  
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS, 
                             value_serializer=\
                             lambda m: json.dumps(m).encode('UTF-8'))
    record_number = 1
    with open(urlFilePath, 'r', errors='ignore') as urlFile:
        records = csv.reader(urlFile, delimiter='\t')
        for record in records:
            #print(record_number)
            producer.send(KAFKA_TOPIC, [record_number, record]).get(timeout=1)
            record_number += 1

        #producer.send(KAFKA_TOPIC, 
        #              key=bytes([line_number]), 
        #              value=url.encode('UTF-8')
        #             ).get(timeout=1)

      
    dtWall = time.time() - t0wall
    return dtWall

if __name__ == '__main__':
    """Command-line execution for producer.py"""
   
    import argparse
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('urlFilePath', 
                        help='Path to the file listing one URL at each line'
                       )
    args = parser.parse_args()
    #Add some input checks here:
    urlFilePath = args.urlFilePath
    dtWall = main(urlFilePath)
    print('DONE in {0:10g} seconds of wall clock time'.format(dtWall))
  
