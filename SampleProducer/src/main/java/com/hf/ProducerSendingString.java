package com.hf;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerSendingString {

    public static void main(String[] args) {

        System.out.println("I am a Kafka Producer");

        // In a proper implementation, this should source from file or a config server.
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG, "1000");


        // Client construction.
        try (KafkaProducer<String, String> producer = new KafkaProducer<>(properties)) {

            System.out.println("Sending message");

            for (int i=0; i<10; i++ ) {


                // create a producer record

                String topic = "demo_java";
                String value = "hello world " + Integer.toString(i);
                String key = "id_" + Integer.toString(i);


                // Construct a String to send over.
                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(topic, key, value);

                producer.send(producerRecord, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // executes every time a record is successfully sent or an exception is thrown
                        if (e == null) {
                            // the record was successfully sent
                            System.out.println("Received new metadata. \n" +
                                    "Topic:" + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp());
                        } else {
                            System.out.println("Error while producing" + e);
                        }
                    }
                });
            }

            System.out.println("Prepare to flush");
            // flush data - synchronous
            producer.flush();
        }
    }
}