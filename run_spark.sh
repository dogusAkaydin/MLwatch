#spark-submit --master local[2] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 spark.py
#spark-submit --master spark://ec2-54-218-154-140.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 --conf "spark.cores.max=18 spark.default.parallelism=18" spark.py
#spark-submit --master spark://ec2-54-218-154-140.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 spark.py
spark-submit --master spark://ec2-54-218-154-140.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2,anguenot/pyspark-cassandra:0.7.0 --conf spark.cassandra.connection.host=10.0.0.13 --conf "spark.cores.max=18" spark.py
