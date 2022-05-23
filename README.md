![download](https://user-images.githubusercontent.com/34875169/169837256-b5cce5b4-0b10-4a5b-82b7-926f10690437.png)
***
How to setup confluent Kafka.
1. [Account Setup](Confluent%20Account.md)
2. [Cluster Setup](ConfluentClusterSetup.md)
3. [Kafka Topic](Confluent%20Topic%20Creation.md)
4. [Obtain secrets](Kafka%20key%20and%20secrets.md)
***

Create a conda environment
```
conda create -p venv python==3.7 -y
```

Activate conda environment
```
conda activate venv
```

To use confluent kafka we need following details from Confluent dashboard.

```
confluentClusterName = ""
confluentBootstrapServers = ""
confluentTopicName = ""
confluentApiKey = ""
confluentSecret = ""
```
Add below library in requirements.txt
```
confluent-kafka[avro,json,protobuf]
pyspark==3.2.1
```

### Read data from kafka topic
Import necessary packages
```
from pyspark.sql import SparkSession
```

Create a spark session object using below snippet.
```
spark_session=SparkSession.builder.master("local[*]").appName("Confluent").getOrCreate()
```
Read data from kafka topic
```
df = (spark_session
          .readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", confluentBootstrapServers)
          .option("kafka.security.protocol", "SASL_SSL")
          .option("kafka.sasl.jaas.config",
                  "org.apache.kafka.common.security.plain.PlainLoginModule  required username='{}' password='{}';".format(confluentApiKey, confluentSecret))
          .option("kafka.ssl.endpoint.identification.algorithm", "https")
          .option("kafka.sasl.mechanism", "PLAIN")
          .option("subscribe", confluentTopicName)
          .option("startingOffsets", "earliest")
          .option("failOnDataLoss", "false")
          .load()
          )
```

process read data from kafka topic

```
df = (df.withColumn('key_str',df['key'].cast('string').alias('key_str')).drop('key').withColumn('value_str',df['value'].cast('string').alias('key_str')))
```

Write data in json file.
```
    query = (df.selectExpr("value_str").writeStream
             .format("json")
             .option("format", "append")
             .trigger(processingTime="5 seconds")
             .option("checkpointLocation", os.path.join("csv_checkpoint"))
             .option("path", os.path.join("json"))
             .outputMode("append")
             .start()
             )
    query.awaitTermination()
```

Write data in csv file
```
    query = (df.writeStream
             .format("csv")
             .option("format", "append")
             .trigger(processingTime="5 seconds")
             .option("checkpointLocation", os.path.join("csv_checkpoint"))
             .option("path", os.path.join("csv"))
             .outputMode("append")
             .start()
             )
    query.awaitTermination()
```
Write data to kafka topic
```
    query = (df.writeStream
             .format("kafka")
             .option("kafka.bootstrap.servers", confluentBootstrapServers)
             .option("kafka.security.protocol", "SASL_SSL")
             .option("kafka.sasl.jaas.config",
                     "org.apache.kafka.common.security.plain.PlainLoginModule  required username='{}' password='{}';".format(
                         confluentApiKey, confluentSecret))
             .option("kafka.ssl.endpoint.identification.algorithm", "https")
             .option("kafka.sasl.mechanism", "PLAIN")
             .option("checkpointLocation", os.path.join("kafka_checkpoint"))
             .option("topic", confluentTopicName).start())


    query.awaitTermination()
```

***
Note: Don't run your python script using python command
use below command to run your script for kafka confluent.
***

To run python script
```commandline
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.1 <scipt_name.py>
```