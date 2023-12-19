package com.hf;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
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
            // Construct a String to send over.
            ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>("hello_topic", "hello Kafka");

            System.out.println("Sending message");
            // send data - asynchronous
            producer.send(producerRecord);

            System.out.println("Prepare to flush");
            // flush data - synchronous
            producer.flush();
        }
    }
}