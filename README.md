# Cloudwick Spark CodeBase

This repository is a collection of Spark examples & use-case implementations for various components of the Spark eco-system including Spark-Core, Spark-Streaming, Spark-SQL, Spark-MLLib.

## What does this repository contains ?

* Spark core examples
    * [WordCount](src/main/scala/com/cloudwick/spark/examples/core/WordCountRunner.scala)
* Spark streaming examples
    * [NetworkWordCount](src/main/scala/com/cloudwick/spark/examples/streaming/local/NetworkWordCount.scala)
    * [NetworkWordCountWindowed](src/main/scala/com/cloudwick/spark/examples/streaming/local/NetworkWordCountWindowed.scala)
    * [RecoverableNetworkWordCount](src/main/scala/com/cloudwick/spark/examples/streaming/local/RecoverableNetworkWordCount.scala)
    * [TwitterPopularTags](src/main/scala/com/cloudwick/spark/examples/streaming/twitter/TwitterPopularTags.scala)
    * [KafkaWordCount](src/main/scala/com/cloudwick/spark/examples/streaming/kafka/KafkaWordCount.scala)
    * [StatefulKafkaWordCount](src/main/scala/com/cloudwick/spark/examples/streaming/kafka/StatefulKafkaWordCount.scala)
    * [KinesisWordCount](src/main/scala/com/cloudwick/spark/examples/streaming/kinesis/KinesisWordCount.scala)
* Spark core use-cases
    * TODO
* Spark streaming use-cases
    * [LogAnalytics](src/main/scala/com/cloudwick/spark/loganalysis/LogAnalyzerRunner.scala) 
        A simple spark streaming use-case to perform apache log analysis which could read data from Kafka & Kinesis performs some analysis and persists the result's to cassandra.
* Testing
    * ScalaTest spec traits for Spark core, streaming and SQL API(s)
    * Embedded [Kafka](src/main/scala/com/cloudwick/spark/embedded/KafkaServer.scala) and [Zookeeper](src/main/scala/com/cloudwick/spark/embedded/ZookeeperServer.scala) embedded server instances for testing
    
## How to download ?

Simplest way is to clone the repository:

```
git clone https://github.com/cloudwicklabs/spark_codebase.git
```
    
## How to run these ?

To run any of these examples or use-cases you have to package them using a uber-jar (most of the examples depend of external dependencies, hence have to be packaged as a assembly jar).

### Building an assembly jar

From the project's home directory

```
sbt assembly
```

### Running using `spark-submit`

[`spark-submit`](https://spark.apache.org/docs/latest/submitting-applications.html) is the simplest way to submit a spark application to the cluster and supports all the cluster manager's like stand-alone, yarn and mesos.

Each of the main class has documentation on how to run it.
