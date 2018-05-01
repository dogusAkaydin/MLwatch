import sys
import json
from kafka import KafkaConsumer

KAFKA_TOPIC = 'demo'
KAFKA_BROKERS = 'localhost:9092'

consumer = KafkaConsumer(KAFKA_TOPIC, 
                         bootstrap_servers=KAFKA_BROKERS,
                         auto_offset_reset='earliest',
                         value_deserializer=\
                         lambda m: json.loads(m.decode('UTF-8'))
                         )

#consumer = KafkaConsumer(KAFKA_TOPIC, 
#                         bootstrap_servers=KAFKA_BROKERS,
#                         )

try:
        for message in consumer:
                    out=message.value
                    print(out)
except KeyboardInterrupt:
        sys.exit

