import json
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
from tensorflow.python.platform import gfile
import tensorflow as tf
import base64
import pyspark_cassandra

# --------------------
# Kafka related initializations:
#KAFKA_TOPIC   = config.KAFKA_CONFIG['topic'] 
#KAFKA_BROKERS = config.KAFKA_CONFIG['brokers'] 

KAFKA_TOPIC = 'demo'
KAFKA_BROKERS = 'localhost:9092'

model_dir = config.MODEL_DIR
# ---------------------
def createContext():
    #sc = SparkContext(master="local[1]", appName="TensorStream")
    sc = SparkContext(appName="TensorStream")
    #sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")
    sc.addPyFile('tflow_readImages.py')
    sc.addPyFile('config.py')
    import tflow_readImages
    infer = tflow_readImages.infer
     
#    def ser(x):
#        #return base64.b64decode(x)
#        try:
#            return x.decode('utf-8')
#        except:
#            pass
    
    model_data_bc = None
    model_path = os.path.join(model_dir, 'classify_image_graph_def.pb') #
    with gfile.FastGFile(model_path, 'rb') as f:
        model_data = f.read()
        model_data_bc = sc.broadcast(model_data)

    ssc = StreamingContext(sc, 2)
    
#    # Define Kafka Consumer
#    kafkaStream = KafkaUtils.createDirectStream(
#                      ssc, 
#                      ['demo'], 
#                      {"metadata.broker.list":'localhost:9092'},
#                      keyDecoder=ser, valueDecoder=ser 
#                                                )

    kafkaStream = KafkaUtils.createDirectStream(
                        ssc, 
                        ['demo'], 
                        {"metadata.broker.list":'localhost:9092'}
                                                  )
    # Count number of requests in the batch
    count_this_batch = kafkaStream.count().map(
                           lambda x:('Requests this batch: %s' % x)
                                              )
    count_this_batch.pprint()
 
    # Count by windowed time period
    #count_window = kafkaStream.countByWindow(20,5).map(
    #                   lambda x:('Requests this window: %s' % x)
    #                                                  )
    #count_window.pprint()

    inferred = kafkaStream.map(lambda x: infer(x, model_data_bc))
    inferred.pprint()
    '''
    # Print the URL requests this batch
    parsed   = kafkaStream.map(lambda m: json.loads(m[1]))
    reparted = parsed.repartition(18)
    inferred = reparted.map(lambda x: infer(x, model_data_bc))
    #inferred = parsed.map(lambda x: infer(x, model_data_bc))
    inferred.pprint()

    # Filter for None outputs
    filtered = inferred.filter(lambda x: not x is None)
    #filtered.pprint()  
  
    #classes = filtered.map(lambda inference: inference['class'])
    #classes.pprint()

    #class_counts = classes.countByValue()
    #class_counts.pprint()

    #countClasses = (classes
    #                    .countByValueAndWindow(20,5)
    #                    .map(lambda x:print('HAHAHAH'))
    #               )
    #countClasses.pprint()
    '''   
    return ssc

ssc = createContext()
#ssc.checkpoint('/tmp/spark_streaming/checkpoint')
#ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v01', lambda: createContext())  
#ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v01', lambda: createContext())  
ssc.start()  
ssc.awaitTermination()