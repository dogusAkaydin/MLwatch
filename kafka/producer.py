from kafka import KafkaProducer

KAFKA_TOPIC = 'testing'
KAFKA_BROKERS = 'localhost:9092'

producer = KafkaProducer(bootstrap_servers=KAFKA_BROKERS)
#producer = KafkaProducer()

# Must send bytes
messages = [b'abc', b'def']

# Send the messages
for m in messages:
    producer.send(KAFKA_TOPIC, m).get(timeout=1)

#producer = KafkaProducer(value_serializer=lambda, 
#                         v: json.dumps(v).encode('utf-8'),
#                         bootstrap_servers=KAFKA_BROKERS)


