from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("myFirstApp").setMaster("local")
sc = SparkContext(conf=conf)

textFile = sc.textFile("hdfs://localhost/examples/foo.txt")
textFile.first()
