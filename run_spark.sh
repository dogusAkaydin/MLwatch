#spark-submit --master local[2] --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 spark.py
spark-submit --master spark://ec2-54-218-154-140.us-west-2.compute.amazonaws.com:7077 --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 spark.py
