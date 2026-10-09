import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.DefaultConnectionKeepAliveStrategy;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opensearch.action.admin.indices.create.CreateIndexRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static org.apache.kafka.clients.consumer.ConsumerConfig.*;
import static org.apache.kafka.clients.consumer.ConsumerConfig.AUTO_OFFSET_RESET_CONFIG;

public class OpenSearchConsumer {
    public static RestHighLevelClient createOpenSearchClient() {
        String connString = "http://localhost:9200";
//        String connString = "https://c9p5mwld41:45zeygn9hy@kafka-course-2322630105.eu-west-1.bonsaisearch.net:443";

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), "http")));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            CredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(auth[0], auth[1]));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort(), connUri.getScheme()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));


        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createKafkaConsumer() {

        String bootstrapServers = "localhost:9092";
        String groupId = "consumer-opensearch-group";

        Properties properties = new Properties();

        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty(KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(GROUP_ID_CONFIG, groupId);
        properties.setProperty(AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ENABLE_AUTO_COMMIT_CONFIG, "false");
        return new KafkaConsumer<>(properties);

    }

    private static final Logger logger = LoggerFactory.getLogger(OpenSearchConsumer.class);
    public static void main(String[] args) throws IOException {
        RestHighLevelClient openSearchClient = createOpenSearchClient();

        KafkaConsumer<String, String> kafkaConsumer = createKafkaConsumer();
        kafkaConsumer.subscribe(Collections.singletonList("wikimedia.recentchange"));
        try(openSearchClient; kafkaConsumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"), RequestOptions.DEFAULT);
            if (!indexExists) {
                CreateIndexRequest createIndexRequest = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
                logger.info("The Wikimedia Index has been created");
            } else {
                logger.info("The Wikimedia Index already exists");
            }

            while(true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(3000));
                int recordsCount = records.count();
                logger.info("The number of records in the consumer is {}", recordsCount);
                for(ConsumerRecord<String, String> record : records) {
                    // send the record to OpenSearch

                    // strategy 1
                    // define an ID using Kafka Record Coordinates.
                    // String id = record.topic() + "_" + record.partition() + "_" + record.offset();

                    // strategy 2
                    // extract meta.id from the Wikimedia JSON data
                    String id = JsonParser.parseString(record.value())
                            .getAsJsonObject()
                            .get("meta")
                            .getAsJsonObject()
                            .get("id")
                            .getAsString();
                    IndexRequest indexRequest = new IndexRequest("wikimedia")
                            .source(record.value(), XContentType.JSON)
                            .id(id);

                    IndexResponse indexResponse = openSearchClient.index(indexRequest, RequestOptions.DEFAULT);

                    logger.info("Index Response ID: {}", indexResponse.getId());
                }

                // commit offsets after the batch is consumed
                kafkaConsumer.commitSync();
                logger.info("Offsets have been committed!");
            }
        }

    }


}
