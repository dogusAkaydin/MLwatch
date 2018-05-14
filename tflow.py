import numpy as np
import tensorflow as tf
from tensorflow.python.platform import gfile
import config

num_top_predictions = 1

def infer(msg, model_data_bc):
    reqID         = msg[0]
    image_path    = msg[1]
    # Creates a new TensorFlow graph of computation and imports the model
    with gfile.FastGFile(image_path, 'rb') as f:
        try:
            image_data = f.read()
        except:
            return 'Can''t read the image.'
        try:
            graph_def = tf.GraphDef()
            graph_def.ParseFromString(model_data_bc.value)
            tf.import_graph_def(graph_def, name='')
            with tf.Session() as sess:
                softmax_tensor = sess.graph.get_tensor_by_name('softmax:0')
                predictions = sess.run(softmax_tensor, {'DecodeJpeg/contents:0': image_data})
                predictions = np.squeeze(predictions)
                top_k = predictions.argsort()[-num_top_predictions:][::-1]
                top_k_probs=[]
                top_k_names=[]
                for node_id in top_k:
                    score = 100*predictions[node_id]
                    top_k_names.append(node_id)
                    top_k_probs.append(score)
                return(top_k_names[0], (1, top_k_probs[0]))
        except:
            err= ('tf_error', (1, 0.0))
            return err
