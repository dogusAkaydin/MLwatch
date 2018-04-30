import sys
from kafka import KafkaConsumer

KAFKA_TOPIC = 'testing'
KAFKA_BROKERS = 'localhost:9092'

consumer = KafkaConsumer(KAFKA_TOPIC, 
                         bootstrap_servers=KAFKA_BROKERS,
                         auto_offset_reset='earliest')

try:
        for message in consumer:
                    print(message.value)
except KeyboardInterrupt:
        sys.exit()

