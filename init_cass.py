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

# --------------------
# Cassandra related initializations:

KEYSPACE = "top1"

cluster = Cluster(['127.0.0.1'])
session = cluster.connect()
# HDA out:
# rows = session.execute("SELECT keyspace_name FROM system.schema_keyspaces")
# HDA in: Function name change  in Cassandra v3.
rows = session.execute("SELECT keyspace_name FROM system_schema.keyspaces")
# HDA done. 
if KEYSPACE in [row[0] for row in rows]:
    log.info("dropping existing keyspace...")
    session.execute("DROP KEYSPACE " + KEYSPACE)

log.info("creating keyspace...")
session.execute("""
    CREATE KEYSPACE %s
    WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
    """ % KEYSPACE)

log.info("setting keyspace...")
session.set_keyspace(KEYSPACE)

log.info("creating table...")
session.execute("""
    CREATE TABLE Top1_Inception (
        reqID text,
        p1 text,
        c1 float,
        url text,
        PRIMARY KEY (reqID)
    )
    """)

sys.exit

