package io.conduktor.demos.kafka;


import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {

    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithKeys.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {
        log.info("Starting producer demo");

        // create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        properties.setProperty("batch.size", "500");

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // create a producer record


        // send data
        for(int j=0; j < 3; j++) {

            for(int i = 0; i < 10; i++) {
                String key = "id-" + i;
                ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", key, "Hello World"+i);
                producer.send(record, new Callback() {

                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception == null) {
                            log.info("Received new metadata. Topic: {} Key : {}, Partition: {} Offset: {} Timestamp: {}", metadata.topic(), key, metadata.partition(), metadata.offset(), metadata.timestamp());
                        } else {
                            log.error("Error while producing", exception);
                        }
                    }
                });
            }
            log.info("-----");
            Thread.sleep(500);
        }



        // flush and close the producer
        producer.flush();
        producer.close();
    }
}