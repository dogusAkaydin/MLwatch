import sys
import os
import json
#from urllib.request import Request, urlopen
#from urllib.error import  URLError
import urllib
import socket
import errno
import numpy as np
from kafka import KafkaConsumer
# --------------------
# Tensorflow related imports
import tensorflow as tf
from datasets import imagenet
from nets import inception
from preprocessing import inception_preprocessing
import re
from tensorflow.python.platform import gfile
# --------------------
# Cassandra related imports

import logging
log = logging.getLogger()
log.setLevel('CRITICAL')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# --------------------
# Cassandra related initializations:

KEYSPACE = "top1"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

#prepared = session.prepare("""
#    INSERT INTO Top5_InceptionV1 (reqID, p1, c1, p2, c2, p3, c3, p4, c4, p5, c5, url)
#    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
#    """)

prepared = session.prepare("""
    INSERT INTO Top1_Inception (reqID, p1, c1, url)
    VALUES (?, ?, ?, ?)
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

model_dir = './inception'
num_top_predictions = 1

class NodeLookup(object):
    """Converts integer node ID's to human readable labels."""
    def __init__(self,
                 label_lookup_path=None,
                 uid_lookup_path=None):
        if not label_lookup_path:
            label_lookup_path = os.path.join(
                model_dir, 'imagenet_2012_challenge_label_map_proto.pbtxt')
        if not uid_lookup_path:
            uid_lookup_path = os.path.join(
                model_dir, 'imagenet_synset_to_human_label_map.txt')
        self.node_lookup = self.load(label_lookup_path, uid_lookup_path)

    def load(self, label_lookup_path, uid_lookup_path):
        """Loads a human readable English name for each softmax node.

        Args:
            label_lookup_path: string UID to integer node ID.
            uid_lookup_path: string UID to human-readable string.

        Returns:
            dict from integer node ID to human-readable string.
        """
        if not gfile.Exists(uid_lookup_path):
            tf.logging.fatal('File does not exist %s', uid_lookup_path)
        if not gfile.Exists(label_lookup_path):
            tf.logging.fatal('File does not exist %s', label_lookup_path)

        # Loads mapping from string UID to human-readable string
        proto_as_ascii_lines = gfile.GFile(uid_lookup_path).readlines()
        uid_to_human = {}
        p = re.compile(r'[n\d]*[ \S,]*')
        for line in proto_as_ascii_lines:
            parsed_items = p.findall(line)
            uid = parsed_items[0]
            human_string = parsed_items[2]
            uid_to_human[uid] = human_string

        # Loads mapping from string UID to integer node ID.
        node_id_to_uid = {}
        proto_as_ascii = gfile.GFile(label_lookup_path).readlines()
        for line in proto_as_ascii:
            if line.startswith('  target_class:'):
                target_class = int(line.split(': ')[1])
            if line.startswith('  target_class_string:'):
                target_class_string = line.split(': ')[1]
                node_id_to_uid[target_class] = target_class_string[1:-2]

        # Loads the final mapping of integer node ID to human-readable string
        node_id_to_name = {}
        for key, val in node_id_to_uid.items():
            if val not in uid_to_human:
                tf.logging.fatal('Failed to locate: %s', val)
            name = uid_to_human[val]
            node_id_to_name[key] = name

        return node_id_to_name

    def id_to_string(self, node_id):
        if node_id not in self.node_lookup:
            return ''
        return self.node_lookup[node_id]

def infer(img_url):
    # Creates a new TensorFlow graph of computation and imports the model
    with gfile.FastGFile( './inception/classify_image_graph_def.pb', 'rb') as f, \
        tf.Graph().as_default() as g:
        model_data = f.read()
        graph_def = tf.GraphDef()
        graph_def.ParseFromString(model_data)
        tf.import_graph_def(graph_def, name='')
        # Loads the image data from the URL:
        image_data = urllib.request.urlopen(img_url, timeout=1.0).read()
        # Runs a tensor flow session that loads the
        with tf.Session() as sess:
            softmax_tensor = sess.graph.get_tensor_by_name('softmax:0')
            predictions = sess.run(softmax_tensor, {'DecodeJpeg/contents:0': image_data})
            predictions = np.squeeze(predictions)
            # Creates node ID --> English string lookup.
            node_lookup = NodeLookup()
            top_k = predictions.argsort()[-num_top_predictions:][::-1]
            top_k_probs=[]
            top_k_names=[]
            for node_id in top_k:
                human_string = node_lookup.id_to_string(node_id)
                score = 100*predictions[node_id]
                #print('{0:<20s} : ({1:2f}%)'.format(human_string, score))
                top_k_names.append(human_string)
                top_k_probs.append(score)
    return list(zip(top_k_names, top_k_probs))

for msg in consumer:
    payload = msg.value
    record_number = payload[0]
    record = payload[1] 
    wnid = record[0]
    url  = record[1]
    print('-'*80)
    print('ReqID: {0} | URL: {1:<s}...'.format(record_number, url[:50]))
    try:
        top_k=infer(url)
        for name, score in top_k:
            print('{0:.<s} : {1:<2.0f}%'.format(name[:50], score))
        #log.info("inserting row %d" % record_number)
        ##session.execute(query, dict(key="key%d" % i, a='a', b='b'))
        session.execute(prepared.bind(("key%d" % record_number, top_k[0][0], top_k[0][1], url)))
    except KeyboardInterrupt:
        future = session.execute_async("SELECT * FROM Top1_Inception")
        try:
            rows = future.result()
        except Exception:
            log.exception()
        
        for row in rows:
            print('Name:{0:.<20s}, Score:{1:4.1f}, URL:{2:.<20s}'.format(row.p1, row.c1, row.url))
        sys.exit()
    except:
        print('Unhandled error')

#if __name__ == '__main__':
#    import argparse
#    parser = argparse.ArgumentParser(description=__doc__)
#    parser.add_argument('img_url', 
#                        help='URL to the image'
#                        )
#
#    args = parser.parse_args()
#    #Add some input checks here:
#    img_url = args.img_url
#   
#    infer_img(img_url)
#

