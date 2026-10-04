package io.conduktor.demos.kafka;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;

public class ConsumerDemoWithShutdown {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class);
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        properties.setProperty(GROUP_ID_CONFIG, "my-consumer-group");
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Collections.singletonList("first_topic"));

        // get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Shutdown hook triggered!");
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));
        try {
            logger.info("--------------Waiting for consumer to start-----------");
            while (true) {
                //logger.info("Polling...");
                consumer.poll(Duration.ofMillis(1000)).forEach(record -> {
                    logger.info("Key: {}, Value: {}, Partition: {}, Offset: {}\"", record.key(), record.value(), record.partition(), record.offset());
                });
            }
        } catch(WakeupException e) {
            logger.info("Consumer woken up!");
        } catch (Exception e) {
            logger.info("Consumer interrupted", e);
        } finally {
            logger.info("Consumer closing...");
            consumer.close();
            logger.info("Consumer closed");
        }
    }
}
