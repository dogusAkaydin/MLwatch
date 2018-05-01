import os
import sys
import time
import csv
import numpy as np
import json
from urllib.request import Request, urlopen
from urllib.error import  URLError
import socket
import errno

timeout = 1 # [sec.]
socket.setdefaulttimeout(timeout)

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

#    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)
    
    # Decoding problem in line 13073007
    line_number = 1
    with open(urlFilePath, 'r', errors='ignore') as urlFile:
        #url = urlFile.readline().rstrip()
        
        records = csv.reader(urlFile, delimiter='\t')

        for record in records:
            wnid = record[0]
            url  = record[1]
            req = Request(url)
            #print(req, wnid, url)
            if True:
                try:
                    image_string = urlopen(req).read()
                except URLError as e:
                    if hasattr(e, 'reason'):
                        #print('We failed to reach a server.')
                        print(line_number, 'URL access error: ', e.reason)
                    elif hasattr(e, 'code'):
                        #print('The server couldn\'t fulfill the request.')
                        print(line_number, 'URL access error: ', e.code)
                    else:
                        print(line_number, 'Unknown URL error: ')
                except socket.error as e:
                    print(line_number, 'Some socket error below')
                    if e.errno == errno.ECONNRESET:
                        print(line_number, 'URL access error: ', e.errno)
                    #else: 
                        #print('Unhandled socket error', e.errno)
                        #pass
                except socket.timeout:
                        print(line_number, 'socket timeout: ')
                else:
                    # everything is fine
                    print(line_number, 'Valid')
                   #producer.send(KAFKA_TOPIC, [line_number,url]).get(timeout=1)
            
            line_number += 1

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
  
