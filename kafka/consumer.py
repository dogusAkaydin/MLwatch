import sys
import json
from kafka import KafkaConsumer
from urllib.request import Request, urlopen
from urllib.error import  URLError
import socket
import errno

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

timeout = 1 # [sec.]
socket.setdefaulttimeout(timeout)

try:
    for msg in consumer:
        payload = msg.value
        record_number = payload[0]
        record = payload[1] 
        wnid = record[0]
        url  = record[1]
        req = Request(url)
        try:
            image_string = urlopen(req).read()
        except URLError as e:
            if hasattr(e, 'reason'):
                #print('We failed to reach a server.')
                print(record_number, 'URL access error: ', e.reason)
            elif hasattr(e, 'code'):
                #print('The server couldn\'t fulfill the request.')
                print(record_number, 'URL access error: ', e.code)
            else:
                print(record_number, 'Unknown URL error: ')
        except socket.error as e:
            if e.errno == errno.ECONNRESET:
                print(record_number, 'URL access error: ', e.errno)
            else: 
                print(record_number, 'Unhandled socket error', e.errno)
        except socket.timeout:
                print(record_number, 'Socket timeout: ')
        else:
            # everything is fine
            print(record_number, 'Valid')
         
        
except KeyboardInterrupt:
        sys.exit

