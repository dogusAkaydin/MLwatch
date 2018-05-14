import os
import config  
# Spark
#os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 pyspark-shell'
os.environ['PYSPARK_PYTHON']='python'
os.environ['PYSPARK_DRIVER_PYTHON']='python'
from pyspark import SparkContext
from pyspark.streaming import StreamingContext  
from pyspark.streaming.kafka import KafkaUtils
# Kafka
from kafka import KafkaConsumer
import json
# Tensorflow
import tensorflow as tf
from tensorflow.python.platform import gfile
# Cassandra
import pyspark_cassandra
import logging
log = logging.getLogger()
log.setLevel('CRITICAL')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from cassandra.query import BatchStatement
from cassandra.query import BatchType

# --------------------
# Kafka related initializations:
KAFKA_TOPIC   = config.KAFKA_CONFIG['topic'] 
KAFKA_BROKERS = config.KAFKA_CONFIG['brokers'] 
MODEL_DIR     = config.MODEL_DIR
IMAGES_DIR    = config.IMAGES_DIR
# --------------------
# Cassandra related initializations:

KEYSPACE = config.KEYSPACE

cluster = Cluster(['54.218.154.140', '52.43.242.90', '52.26.55.216', '34.209.1.83' ])
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

#insert_logs  = session.prepare("INSERT INTO logs  (reqID, p1, c1, path) VALUES (?, ?, ?, ?)")
#insert_stats = session.prepare("INSERT INTO stats (prediction, count, acc_score) VALUES (?, ?, ?)")
update_stats = session.prepare("UPDATE stats SET count = count + ?, acc_score = acc_score + ? WHERE prediction = ? ")

def sendCassandra(item):
    print("send to cassandra")
    cluster = Cluster(['54.218.154.140', '52.43.242.90', '52.26.55.216', '34.209.1.83' ])
    session = cluster.connect()
    session.execute('USE ' + config.KEYSPACE)

    count = 0

    # batch insert into cassandra database
    #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
    batch = BatchStatement( batch_type=BatchType.COUNTER)
    for record in item:
        #batch.add(insert_log, (int(record[0]), record[1], float(record[2]), record[3]))
        #batch.add(insert_stats, (str(record[0]), int(record[1][0]), float(record[1][1])))
        batch.add(update_stats, (int(record[1][0]), float(record[1][1]), str(record[0]) ))

        # split the batch, so that the batch will not exceed the size limit
        count += 1
        if count % 500 == 0:
            session.execute(batch)
            #batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            batch = BatchStatement( batch_type=BatchType.COUNTER)

    # send the batch that is less than 500            
    session.execute(batch)
    session.shutdown()

def createContext():
    #sc = SparkContext(master="local[1]", appName="TensorStream")
    sc = SparkContext(appName="TensorStream")
    #sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")
    sc.addPyFile('tflow.py')
    sc.addPyFile('config.py')
    import tflow
    infer = tflow.infer
    
    model_data_bc = None
    model_path = os.path.join(MODEL_DIR, 'classify_image_graph_def.pb') #
    with gfile.FastGFile(model_path, 'rb') as f, \
        tf.Graph().as_default() as g:
        model_data = f.read()
        model_data_bc = sc.broadcast(model_data)
    #model_data_bc = 0

    ssc = StreamingContext(sc, 30)
    
    # Define Kafka Consumer
    kafkaStream = KafkaUtils.createDirectStream(
                      ssc, 
                      [KAFKA_TOPIC], 
                      {"metadata.broker.list":'localhost:9092'}
                                                )
    #kafkaStream.pprint()
    # Count number of requests in the batch
    count_this_batch = kafkaStream.count().map(
                           lambda x:('Number of requests this batch: %s' % x)
                                             )
    count_this_batch.pprint()
 
    # Count by windowed time period
    #count_window = kafkaStream.countByWindow(20,5).map(
    #                   lambda x:('Requests this window: %s' % x)
    #                                                  )
    #count_window.pprint()

    # Print the path requests this batch
    paths  = kafkaStream.map(lambda m: (json.loads(m[1])[0], json.loads(m[1])[1]))
    #paths.pprint()
    reparted = paths.repartition(18)
    #reparted.pprint()
    
    #inferred = paths.map(lambda x: infer(x, model_data_bc))
    inferred = reparted.map(lambda x: infer(x, model_data_bc))
    #inferred.pprint()
    
    reduced = inferred.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
    #reduced.pprint()

    reduced.foreachRDD(lambda rdd: rdd.foreachPartition(sendCassandra))
    
    return ssc

ssc = createContext()
ssc.start()  
ssc.awaitTermination()
