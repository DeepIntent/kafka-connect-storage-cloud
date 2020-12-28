# DeepIntent
Custom protoparquet format is added for 5.4.1 and 5.5.1 versions to DI-5.4.1 and DI-5.5.1 branches.

After building project and generating jar, jar file should be placed into confluent-x.y.z/share/java/kafka-connect-s3 directory.
Also, the following libraries should be placed into confluent-x.y.z/share/java/kafka-connect-s3 directory:
 - elephant-bird-core-4.4.jar
 - parquet-protobuf-1.11.1.jar
 - protobuf-objects-1.0.100-SNAPSHOT.jar
 - protobuf-java-3.5.1.jar

# Kafka Connect Connector for S3
[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=shield)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_shield)


*kafka-connect-storage-cloud* is the repository for Confluent's [Kafka Connectors](http://kafka.apache.org/documentation.html#connect)
designed to be used to copy data from Kafka into Amazon S3. 

## Kafka Connect Sink Connector for Amazon Simple Storage Service (S3)

Documentation for this connector can be found [here](http://docs.confluent.io/current/connect/connect-storage-cloud/kafka-connect-s3/docs/index.html).

Blogpost for this connector can be found [here](https://www.confluent.io/blog/apache-kafka-to-amazon-s3-exactly-once).

# Development

To build a development version you'll need a recent version of Kafka 
as well as a set of upstream Confluent projects, which you'll have to build from their appropriate snapshot branch.
See [the kafka-connect-storage-common FAQ](https://github.com/confluentinc/kafka-connect-storage-common/wiki/FAQ)
for guidance on this process.

You can build *kafka-connect-storage-cloud* with Maven using the standard lifecycle phases.


# Contribute

- Source Code: https://github.com/confluentinc/kafka-connect-storage-cloud
- Issue Tracker: https://github.com/confluentinc/kafka-connect-storage-cloud/issues


# License

This project is licensed under the [Confluent Community License](LICENSE).


[![FOSSA Status](https://app.fossa.io/api/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud.svg?type=large)](https://app.fossa.io/projects/git%2Bhttps%3A%2F%2Fgithub.com%2Fconfluentinc%2Fkafka-connect-storage-cloud?ref=badge_large)
