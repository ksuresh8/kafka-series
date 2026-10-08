package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemo {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemo.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(GROUP_ID_CONFIG, "my-consumer-group");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("first_topic"));

        while(true){
            logger.info("Polling...");
            consumer.poll(Duration.ofMillis(1000)).forEach(record -> {
                logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}\"", record.key(), record.value(), record.partition(), record.offset());
            });
        }

    }
}
