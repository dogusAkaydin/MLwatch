import sys
import os
import json
from urllib.request import Request, urlopen
from urllib.error import  URLError
import socket
import errno
from kafka import KafkaConsumer
# --------------------
# Tensorflow related imports
import tensorflow as tf
from datasets import imagenet
from nets import inception
from preprocessing import inception_preprocessing
# --------------------
# Cassandra related imports

import logging
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# --------------------
# Cassandra related initializations:

KEYSPACE = "top5"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

#query = SimpleStatement("""
#    INSERT INTO Top5_InceptionV1 (reqID, p1, c1, p2, c2, p3, c3, p4, c4, p5, c5, url)
#    VALUES (%(key)s, %(a)s, %(b)s)
#    """, consistency_level=ConsistencyLevel.ONE)

prepared = session.prepare("""
    INSERT INTO Top5_InceptionV1 (reqID, p1, c1, p2, c2, p3, c3, p4, c4, p5, c5, url)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

# --------------------
# Kafka related initializations:
KAFKA_TOPIC = 'demo'
KAFKA_BROKERS = 'localhost:9092'

consumer = KafkaConsumer(KAFKA_TOPIC, 
                         bootstrap_servers=KAFKA_BROKERS,
                         auto_offset_reset='earliest',
                         value_deserializer=\
                         lambda m: json.loads(m.decode('UTF-8'))
                         )

timeout = 1 # [sec.]
socket.setdefaulttimeout(timeout)

# --------------------
# Tensorflow related work
slim = tf.contrib.slim

image_size = inception.inception_v1.default_image_size

# --------------------

with tf.Graph().as_default():
    #with slim.arg_scope(inception.inception_v1_arg_scope()):
        try:
            for msg in consumer:
                payload = msg.value
                record_number = payload[0]
                record = payload[1] 
                wnid = record[0]
                url  = record[1]
                req = Request(url)
                try:
                    image_string = urlopen(req).read()
                except URLError as e:
                    if hasattr(e, 'reason'):
                        #print('We failed to reach a server.')
                        print(record_number, 'URL access error: ', e.reason)
                    elif hasattr(e, 'code'):
                        #print('The server couldn\'t fulfill the request.')
                        print(record_number, 'URL access error: ', e.code)
                    else:
                        print(record_number, 'Unknown URL error: ')
                except socket.error as e:
                    if e.errno == errno.ECONNRESET:
                        print(record_number, 'URL access error: ', e.errno)
                    else:
                        print(record_number, 'Unhandled socket error', e.errno)
                except socket.timeout:
                        print(record_number, 'Socket timeout: ')
                else:
                    # everything is fine
                    print(record_number, 'Valid', url)
                    image = tf.image.decode_jpeg(image_string, channels=3)
                    processed_image = inception_preprocessing.preprocess_image(image, image_size, image_size, is_training=False)
                    processed_images  = tf.expand_dims(processed_image, 0)
                    # Create the model, use the default arg scope to configure the batch norm parameters.
                    with slim.arg_scope(inception.inception_v1_arg_scope()):
                        #reuse=True
                        #tf.AUTO_REUSE
                        logits, _ = inception.inception_v1(processed_images, num_classes=1001, is_training=False, reuse=tf.AUTO_REUSE)
                    probabilities = tf.nn.softmax(logits)
                    checkpoints_dir='slim_pretrained' 
                    init_fn = slim.assign_from_checkpoint_fn(
                        os.path.join(checkpoints_dir, 'inception_v1.ckpt'),
                        slim.get_variables_to_restore())

                    with tf.Session() as sess:
                        init_fn(sess)
                        np_image, probabilities = sess.run([image, probabilities])
                        probabilities = probabilities[0, 0:]
                        sorted_inds = [i[0] for i in sorted(enumerate(-probabilities), key=lambda x:x[1])]
                    names = imagenet.create_readable_names_for_imagenet_labels()
                    #result_text=''
                    top5_probs=[]
                    top5_names=[]
                    for i in range(5):
                        index = sorted_inds[i]
                        top5_probs.append(int(100*probabilities[index]))
                        top5_names.append(names[index])
                        
                        print('Probability %0.2f%% => [%s]' % (int(100*probabilities[index]), names[index]))
                    log.info("inserting row %d" % record_number)
                    #session.execute(query, dict(key="key%d" % i, a='a', b='b'))
                    session.execute(prepared.bind(("key%d" % record_number,
                                                   top5_names[0], top5_probs[0], 
                                                   top5_names[1], top5_probs[1], 
                                                   top5_names[2], top5_probs[2], 
                                                   top5_names[3], top5_probs[3], 
                                                   top5_names[4], top5_probs[4], 
                                                   url
                                                 )))

        except KeyboardInterrupt:
               

            future = session.execute_async("SELECT * FROM Top5_InceptionV1")
            #log.info("key\tcol1\tcol2")
            #log.info("---\t----\t----")

            try:
                rows = future.result()
            except Exception:
                log.exception()
            
            for row in rows:
                #log.info(row)
                print(row.p1, row.c1, row.url)
            

            #session.execute("DROP KEYSPACE " + KEYSPACE)
            sys.exit

