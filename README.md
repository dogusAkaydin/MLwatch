# Glia: An infrastructure for automated ML workflows  

## What?
Glia is a data engineering infrastructure to enable continuous monitoring and training of machine learning (ML) models.

## Why?
Unpredictable shifts in live data is a common concern for many production ML models. The ML model developers may not always have the metrics ready to help them decide when to retrain their models. As a result, they retrain the models at frequencies which may be more or less than actually needed. In additon, once they decide to retrain, they may not be able to incorporate new training data in their models. Glia aims to automate these tasks at scale.

## How?

![Proposed architecture](./arch.jpg)

Data: Image-Net (14M images), Open Image dataset (9M images), or generally some labeled dataset with a pretrained model. 
Ingestion: Kafka
ML Model: The pretrained model of the data set. (Inception, Resnet, VGG, ...)

