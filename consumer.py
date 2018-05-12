#! /usr/bin/python
import config
import time
from kafka import KafkaConsumer
# --------------------
# Kafka related initializations:

KAFKA_TOPIC   = config.KAFKA_CONFIG['topic']
KAFKA_BROKERS = config.KAFKA_CONFIG['brokers']

consumer = KafkaConsumer(KAFKA_TOPIC, 
                         bootstrap_servers=KAFKA_BROKERS,
                         auto_offset_reset='earliest')

t0wall = time.time()
record_number = 1
for msg in consumer:
    payload = msg.value
    print('{}'.format(record_number))
    record_number += 1

dtWall = time.time() - t0wall
print(dtWall)

if __name__ == '__main__':
    main()
