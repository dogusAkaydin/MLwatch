import os
## -- py3 --
#import urllib
#from urllib.error import  URLError
## ---------
import urllib2
from urllib2 import URLError
## ---------
import socket
import errno
import re
import numpy as np
import tensorflow as tf
import config

num_top_predictions = 1

timeout = 0.1 # [sec.]
#socket.setdefaulttimeout(timeout) #This messes up Spark's connections elsewhere
socket.setdefaulttimeout(None)

## ------------------------------------------------------
## Minimal Working Example for Debugging
#def infer(msg):
#  #import tensorflow as tf
#  return msg[0]
#
### ------------------------------------------------------
##

## ------------------------------------------------------
## Minimal Working Example for Debugging
#def infer(msg):
#  import tensorflow as tf
#  with tf.Graph().as_default() as g:
#    hello = tf.constant(str(msg[0])+msg[1][1]+'++++++++++++++++++++', name="hello_constant")
#    with tf.Session() as sess:
#      return sess.run(hello)
#
## ------------------------------------------------------
##

def infer(msg, model_data_bc):
    record_number = msg[0]
    record = msg[1]
    wnid = record[0]
    url  = record[1]
    # Creates a new TensorFlow graph of computation and imports the model
    try:
#        # Loads the image data from the URL:
#        #image_data = urllib.request.urlopen(url, timeout=timeout).read() # py2
        image_data = urllib2.urlopen(url, timeout=timeout).read()
        graph_def = tf.GraphDef()
        #graph_def.ParseFromString(model_data)
        graph_def.ParseFromString(model_data_bc.value)
        tf.import_graph_def(graph_def, name='')
        # Runs a tensor flow session that loads the
        with tf.Session() as sess:
            softmax_tensor = sess.graph.get_tensor_by_name('softmax:0')
            predictions = sess.run(softmax_tensor, {'DecodeJpeg/contents:0': image_data})
            predictions = np.squeeze(predictions)
            top_k = predictions.argsort()[-num_top_predictions:][::-1]
            top_k_probs=[]
            top_k_names=[]
            ## Creates node ID --> English string lookup.
            #node_lookup = NodeLookup()
            for node_id in top_k:
                #human_string = node_lookup.id_to_string(node_id)
                score = 100*predictions[node_id]
                #print('{0:<20s} : ({1:2f}%)'.format(human_string, score))
                #top_k_names.append(human_string)
                top_k_names.append(node_id)
                top_k_probs.append(score)
            #return list(zip(top_k_names, top_k_probs))
            #return dict(zip(top_k_names, top_k_probs))
            #return (top_k_names[0], top_k_probs[0])
            return {'class':top_k_names[0], 'score':top_k_probs[0]}
#    except URLError as e:
#        if hasattr(e, 'reason'):
#            err=(record_number, 'URL access error: ', e.reason)
#            pass
#        elif hasattr(e, 'code'):
#            err=(record_number, 'URL access error: ', e.code)
#            pass
#        else:
#            err=(record_number, 'Unknown URL error: ')
#            pass
#        return err
#    except socket.timeout as e:
#        err=(record_number, 'URL access error: ', 'Socket timed out')
#        return err
#    except socket.error as e:
#        if e.errno == errno.ECONNRESET:
#            err=(record_number, 'URL access error: ', e.errno)
#            pass
#        else:
#            err=(record_number, 'Unhandled socket error', e.errno)
#            pass
#        return err
    except:
        err=('Invalid image data')
        return err


#--------------- NOT USED
#class NodeLookup(object):
#    """Converts integer node ID's to human readable labels."""
#    def __init__(self,
#                 label_lookup_path=None,
#                 uid_lookup_path=None):
#        if not label_lookup_path:
#            label_lookup_path = os.path.join(
#                model_dir, 'imagenet_2012_challenge_label_map_proto.pbtxt')
#        if not uid_lookup_path:
#            uid_lookup_path = os.path.join(
#                model_dir, 'imagenet_synset_to_human_label_map.txt')
#        self.node_lookup = self.load(label_lookup_path, uid_lookup_path)
#
#    def load(self, label_lookup_path, uid_lookup_path):
#        """Loads a human readable English name for each softmax node.
#
#        Args:
#            label_lookup_path: string UID to integer node ID.
#            uid_lookup_path: string UID to human-readable string.
#
#        Returns:
#            dict from integer node ID to human-readable string.
#        """
#        if not gfile.Exists(uid_lookup_path):
#            tf.logging.fatal('File does not exist %s', uid_lookup_path)
#        if not gfile.Exists(label_lookup_path):
#            tf.logging.fatal('File does not exist %s', label_lookup_path)
#
#        # Loads mapping from string UID to human-readable string
#        proto_as_ascii_lines = gfile.GFile(uid_lookup_path).readlines()
#        uid_to_human = {}
#        p = re.compile(r'[n\d]*[ \S,]*')
#        for line in proto_as_ascii_lines:
#            parsed_items = p.findall(line)
#            uid = parsed_items[0]
#            human_string = parsed_items[2]
#            uid_to_human[uid] = human_string
#
#        # Loads mapping from string UID to integer node ID.
#        node_id_to_uid = {}
#        proto_as_ascii = gfile.GFile(label_lookup_path).readlines()
#        for line in proto_as_ascii:
#            if line.startswith('  target_class:'):
#                target_class = int(line.split(': ')[1])
#            if line.startswith('  target_class_string:'):
#                target_class_string = line.split(': ')[1]
#                node_id_to_uid[target_class] = target_class_string[1:-2]
#
#        # Loads the final mapping of integer node ID to human-readable string
#        node_id_to_name = {}
#        for key, val in node_id_to_uid.items():
#            if val not in uid_to_human:
#                tf.logging.fatal('Failed to locate: %s', val)
#            name = uid_to_human[val]
#            node_id_to_name[key] = name
#
#        return node_id_to_name
#
#    def id_to_string(self, node_id):
#        if node_id not in self.node_lookup:
#            return ''
#        return self.node_lookup[node_id]
#
