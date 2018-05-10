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
# --------------------
# Kafka related initializations:
#KAFKA_TOPIC   = config.KAFKA_CONFIG['topic'] 
#KAFKA_BROKERS = config.KAFKA_CONFIG['brokers'] 

KAFKA_TOPIC = 'demo'
KAFKA_BROKERS = 'localhost:9092'

# ---------------------
def createContext():
    import myTF
    infer = myTF.infer
    sc = SparkContext(master="local[1]", appName="TensorStream")
    #sc.setLogLevel("WARN")
    sc.setLogLevel("ERROR")
    sc.addPyFile('myTF.py')
    
    ssc = StreamingContext(sc, 1)
    
    # Define Kafka Consumer
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

    # Print the URL requests this batch
    parsed = kafkaStream.map(lambda m: json.loads(m[1]))

    inferred = parsed.map(infer)

    # Filter for None
    filtered = inferred.filter(lambda x: not x is None)
    filtered.pprint()  
  
    #kvStream= (filtered
    #                  .keyBy(lambda d: d.get('class'))
    #                  .reduceByKey(lambda x, y: x)
    #                  .values())

    #kvStream.pprint()

    #classes = filtered.map(lambda inference: inference['class'])
    #classes.pprint()

    #class_counts = classes.countByValue()
    #class_counts.pprint()

    #countClasses = (classes
    #                    .countByValueAndWindow(20,5)
    #                    .map(lambda x:print('HAHAHAH'))
    #               )
    #countClasses.pprint()
   
    return ssc

ssc = createContext()
#ssc.checkpoint('/tmp/spark_streaming/checkpoint')
#ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v01', lambda: createContext())  
#ssc = StreamingContext.getOrCreate('/tmp/checkpoint_v01', lambda: createContext())  
ssc.start()  
ssc.awaitTermination()
