#! /usr/bin/python
import os
import sys
import time
import csv
import numpy as np
import json
import config
import codecs #python2 
from kafka import KafkaProducer
from tensorflow.python.platform import gfile


def main():
    """Carry-out the main routine, return the wall clock time passed."""
    t0wall = time.time()
    
    #urlFilePath = os.path.join(config.MODEL_DIR, 'fall11_urls.txt')

    root_dir = '/home/ubuntu/.kaggle/competitions/imagenet-object-detection-challenge/ILSVRC/Data/DET/train/ILSVRC2013_train/n08256735'
    
    record_number = 1

    KAFKA_TOPIC   = config.KAFKA_CONFIG['topic'] 
    KAFKA_BROKERS = config.KAFKA_CONFIG['brokers'] 
 
    producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS) 


    for directory, subdirectories, files in os.walk(root_dir):
        for filePath in files:
            image_path=os.path.join(directory,filePath)
            #with codecs.open(urlFilePath, 'r', errors='ignore') as urlFile: #py2
            #with codecs.open(image_path, 'r') as image_file: #py2
            with gfile.FastGFile(image_path, 'r') as image_file:
                image=image_file.read()
                print(image)
                #producer.send(KAFKA_TOPIC,bytearray(image).get(timeout=1)
                producer.send(KAFKA_TOPIC,image).get(timeout=1)
                record_number += 1
                time.sleep(1)

    dtWall = time.time() - t0wall
    return dtWall

if __name__ == '__main__':
    """Command-line execution for producer.py"""
    
    dtWall = main()
    print('DONE in {0:10g} seconds of wall clock time'.format(dtWall))
  
