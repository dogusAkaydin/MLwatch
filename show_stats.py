#! /usr/bin/python
import sys
import config
# --------------------
# Cassandra related imports

import logging
log = logging.getLogger()
log.setLevel('INFO')
handler = logging.StreamHandler()
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s: %(message)s"))
log.addHandler(handler)

from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# --------------------
# Cassandra related initializations:

KEYSPACE = config.KEYSPACE

cluster = Cluster(['localhost'])
session = cluster.connect()
# HDA out:
# rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
# HDA in: Function name change  in Cassandra v3.
rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
print(rows[0])
# HDA done. 

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

future = session.execute_async("SELECT * FROM stats")
#try:
#    rows = future.result()
#except Exception:
#    log.exception()

rows = future.result()
for row in rows:
    #print('Prediction = {0:3s}, Count= {1:5d}, Accumulated_Score = {2:10d}'.format(row.prediction, row.count, row.acc_score))
    print('{:10d}'.format(row.acc_score))
sys.exit()





sys.exit

