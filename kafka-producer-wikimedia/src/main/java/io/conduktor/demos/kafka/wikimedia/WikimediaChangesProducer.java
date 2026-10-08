package io.conduktor.demos.kafka.wikimedia;

import com.launchdarkly.eventsource.ConnectStrategy;
import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;
import com.launchdarkly.eventsource.background.BackgroundEventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        String bootstrapServers = "localhost:9092";
        String topic = "wikimedia.recentchange";

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        /*
         * acks = -1 (ALL)
         * delivery.timeout.ms = 120000
         * enable.idempotence = true
         * max.in.flight.requests.per.connection = 5
         * retries = 2147483647 (Integer.MAX_VALUE)
         * retry.backoff.ms = 100
         */

        // High throughput producer (at the expense of a bit of latency and CPU usage)
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 *1024)); // 32 KB batch size
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(props);
        BackgroundEventHandler eventHandler = new WikimediaChangeHandler(kafkaProducer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder eventSourceBuilder = new EventSource.Builder(
                ConnectStrategy.http(URI.create(url))
                        .header("User-Agent", "Mozilla/5.0 (compatible; KafkaProducer/1.0; +https://example.com)")
        );
        try (BackgroundEventSource eventSource = new BackgroundEventSource.Builder(eventHandler, eventSourceBuilder).build()) {
            eventSource.start();
        }

        //TimeUnit.MINUTES.sleep(10);
        TimeUnit.SECONDS.sleep(5);
    }
}
