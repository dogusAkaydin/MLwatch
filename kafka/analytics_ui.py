import sys
import os
import json
import socket
import errno
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

KEYSPACE = "top5"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

future = session.execute_async("SELECT * FROM Top5_InceptionV1")
#log.info("key\tcol1\tcol2")
#log.info("---\t----\t----")

try:
    rows = future.result()
except Exception:
    log.exception()

for row in rows:
    #log.info(row)
    print(row.p1, row.c1, row.url)

#session.execute("DROP KEYSPACE " + KEYSPACE)
sys.exit

