# MLwatch: Keep an eye on your predictions.  

MLwatch is a pipeline to serve and monitor of machine learning (ML) models at scale.

## Motivation
Being able to serve online ML predictions at scale is needed in many use cases such as autonomous vehicles, anomaly detection, estimated time of arrival predictions, etc. 
Furthermore, unpredictable shifts in live data is a common concern for many production ML models once they are deployed. 
The ML model developers may not always have the metrics ready to help them decide when to retrain their models. 
As a result, they retrain the models at frequencies which may be more or less than actually needed. 
MLwatch aims to produce near-real-time statistics on the predictions of the model to help ML model developers monitor fitness of the model to shifts in the incoming data.

## The DE challenges

The primary data engineering challenge for this system to integrate the ML component into the datapipleline so that a high-number of inferences can be made (e.g. 1000 inference/sec.) with a relatively small time delay ( <1 min). 
While a sub-minute delay is not necessary for monitoring the performance metrics, it would be useful if an anomaly-detection mechanism would be tied to this pipeline. 

## The pipeline

![The architecture](./visuals/arch.jpg)

* Data: Images from the test set of ImageNet Object Recognition Challenge dataset on Kaggle.
* Resources: 4 m4.large nodes (2 vCPUs and 8GB memory each), 1 serving as a master node 3 serving as workers with 3x oversubscription (`SPARK_WORKER_CORES=6`).  
* Ingestion: Kafka producers writing the paths to the images in a topic at a controllable rate.
* Processing: Tensorflow generator instances using Inception V3 pretrained model, created and managed by Spark Streaming. 
* Database: Cassandra to store prediction frequency and average confidence for each label.
I chose Cassandra because I knew the queries I needed and I wanted take advantage of high write speed and relatively easy scalability of Cassandra. 
* User Interface: Dash to display the frequency and confidence levels over the set of 1,000 labels based on the all the predictions to date.

## Tackling the challenges (one bottleneck at a time)

My approach to this problem was to first build the pipeline from end-to-end then optimize. 
As such, I iterated several times to remove a particular bottleneck which resulted in another one elsewhere in the pipeline.
For example, in my first iteration I would pass URLs to the images from the Kafka producer to Tensorflow instance.
While I was able to get some data moving thorough the pipeline, collection of data from URLs would cause lots of delays due to slow/missing/bad data.

In my second iteration, I downloaded a subset set of ImageNet images (~200KB each) and passed them through the Kafka producer. 
As I anticipated, the serialization/deserialization overhead between Kafka and Spark became the new bottleneck.

I quickly moved on to the fourth iteration, in which I passed the paths instead of the actual images from Kafka to Spark.
Each Tensorflow instance then would read the images directly from the filesystem.
This modification made the Tensorflow inference the new bottleneck, with an rate of 2-3 inferences per second with Spark job dying after a few hundred inferences.
The reason for the slow response and unstable behavior was due to a naive way of mapping the inferencerequests:
```
paths.map(lambda x: tflow.infer(x, broadcast_data))
```
where tflow.py looks like

```
# Several import statements here.
def infer(path, broadcast_data)
#Complex graph instantiations and computations here
    return prediction
```
This method would map each record separately to each worker which would create and destroy a Tensorflow instance upon every single inference request, which causes a large amount of overhead per inference.

In my fourth iteration I researched how the Tensorflow instances can be saved and reused for a series of requests.
I found out that I could map an entire partition of records to each worker by using `.mapPartitions` method instead of `.map`:

```
paths.mapPartitions(lambda x: tflow.infer(x, broadcast_data))
```
and convert my Tensorflow function to a generator using the `yield` statement instead of `return`:

```
# Several import statements here.
def infer(path, broadcast_data)
#Complex graph instantiations and computations here
    yield prediction
```

This modification massively increased the inference rate to ~150 inferences/sec. (or ~50 inferences/sec. per node).
I was able to repartition each batch as high as `.repartition(108)` and process over 2,000 requests under 15 seconds for several batches.
This iteration was the most challenging and rewarding improvement I have done on the pipeline thus far.
This is also where I had to stop improving on the project and move on to the rest of the interview preparations. 
The current bottleneck is the `.reduceByKey` operation, which presumably suffers from the lack of memory. 
I have also observed that Cassandra service would go down in any one of the worker nodes during the process. 
I played around with related Spark configuration settings related to memory allocations to leave some room for the rest of the operations. 
However, I could not make the rest of the pipeline to keep up with the high-rate of inferences completed; I was able sustain only about 15 inferences/sec. across the whole pipeline down to Cassandra. 
I presume keeping up with the 150 inferences/second might require a more careful allocation of resources within each node or perhaps scaling up each node (e. g. m4.xlarge) to make room for the rest of the operations in the pipeline.

