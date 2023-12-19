Ref:

How to start Kafka from binary:
https://kafka.apache.org/quickstart

KAFKA_CLUSTER_ID="$(bin/kafka-storage.sh random-uuid)"
bin/kafka-storage.sh format -t $KAFKA_CLUSTER_ID -c config/kraft/server.properties
bin/kafka-server-start.sh config/kraft/server.properties

Old reference, with v0.9 API
https://www.tutorialspoint.com/apache_kafka/apache_kafka_simple_producer_example.htm

Better producer reference:
https://www.conduktor.io/kafka/complete-kafka-producer-with-java/

Kafka stream:
https://www.baeldung.com/java-kafka-streams