# MLwatch: Keep an eye on your predictions.  

## What?
MLwatch is a pipeline to enable continuous service and monitoring of machine learning (ML) models.

## Why?
Being able to serve online ML predictions at scale is important in many use cases such as autonomous vehicles, estimated time of arrival predictions, anomaly detection, etc. Furthermore, unpredictable shifts in live data is a common concern for many production ML models once they are deployed. The ML model developers may not always have the metrics ready to help them decide when to retrain their models. As a result, they retrain the models at frequencies which may be more or less than actually needed. MLwatch aims to monitor the statistics of the predictions to help ML model developers monitor the response of their model to changes in the incoming data.

## Target Specifications:
* Online prediction response time: <5 sec.
* Online prediction rate         : O(1,000) per sec.

## Challenges

The primary data engineering challenge for this system is high-availability and near-realtime response to a high-rate of prediction requests.

## How?

![Proposed architecture](./visuals/arch.jpg)

* Data: Images from the test set of ImageNet Object Recognition Challenge dataset on Kaggle
* Ingestion: Kafka producers writing the path of the images on the file system
* Processing: Tensorflow instances using Inception V3 pretrained model, called and managed by Spark Streaming
* Database: Cassandra to store prediction frequency and average confidence for each label
* User Interface: Dash to display the frequency and confidence levels over Inceptions' label set using the all the predictions done to date.  

