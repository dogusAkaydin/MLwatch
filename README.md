# Glia: An infrastructure for automated ML workflows  

## What?
Glia is a data engineering infrastructure to enable continuous service, monitoring and training of machine learning (ML) models.

## Why?
Unpredictable shifts in live data is a common concern for many production ML models. The ML model developers may not always have the metrics ready to help them decide when to retrain their models. As a result, they retrain the models at frequencies which may be more or less than actually needed. In additon, once they decide to retrain, they may not be able to incorporate new training data in their models. Glia aims to automate these tasks at scale.

## Specifications:
* Online prediction response time: <5 sec.
* Online prediction rate         : O(1,000) per sec.
* Top-1 accuracy                 : >75%
* Batch processing mode          : Run when average Top-1 accuracy <90%

## Challenges

The primary data engineering challenge for this system is high-availability and near-realtime response to a high-rate of prediction requests. The other data engineering challenge is deciding when to retrain the model and executing retraining in batch mode. 



## How?

![Proposed architecture](./arch.jpg)

* Data: URL's to Image-Net (14M images), Open Image dataset (9M images), or generally some labeled dataset with a pretrained model. 
* Ingestion: Kafka producers fetching URL's. For demo, the URL are read-off a text file.
* ML Model: The pretrained model of the data set. (Inception, Resnet, VGG, ...) encapsulated in multiple Kafka consumers in ca consumer group


