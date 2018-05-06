import sys
import os
import json
import socket
import errno
import numpy as np
import math
import time
import matplotlib
from matplotlib import pyplot as plt
from matplotlib.ticker import FuncFormatter
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

def to_percent(y, position):
    # Ignore the passed in position. This has the effect of scaling the default
    # tick locations.
    s = str(int(y/npred*100))
    # The percent symbol needs escaping in latex
    if matplotlib.rcParams['text.usetex'] is True:
        return s + r'$\%$'
    else:
        return s + '%'

try:    
    #while True:
        try:
            rows = future.result()
        except Exception:
            log.exception()
        all_c1=np.array([])
        for row in rows:
            #log.info(row)
            #print(row.p1, row.c1, row.url)
            all_c1=np.append(all_c1, row.c1)

        print(all_c1)

        bins = np.linspace(math.ceil(min(all_c1)), 
                           math.floor(max(all_c1)),
                                                10) # fixed number of bins
        
        plt.figure(figsize=(20,20))
        plt.xlim([min(all_c1), max(all_c1)])

        #plt.hist(all_c1, bins=bins, alpha=1.0, normed=True, stacked=True)
        plt.hist(all_c1, bins=bins, alpha=1.0)
        plt.title('Confidence Histogram', fontsize=36)
        plt.xlabel('% Prediction Confidence', fontsize=36)
        plt.ylabel('Prevalence', fontsize=36)
        plt.xticks(fontsize=36)
        plt.yticks(fontsize=36)
       
        # Create the formatter using the function to_percent. This multiplies all the
        # default labels by 100, making them all percentages
        npred = len(all_c1)
        print(npred)
        formatter = FuncFormatter(to_percent)
        
        # Set the formatter
        plt.gca().yaxis.set_major_formatter(formatter)

        plt.show()
     #   time.sleep(5)    
        

except KeyboardInterrupt:
    sys.exit

