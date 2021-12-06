package com.github.iamyacin.kafka.elasticsearch;

import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


// Consumer auto commit
public class ElasticSearchConsumer {

    //method to create the kafka consumer
    public static KafkaConsumer<String, String> createConsumer(String topic) {
        String bootstrapServers = "localhost:9092";
        String groupID = "JavaGroupID-2";

        //Consumer Config :
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupID);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // This property is for enable manual commit
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // This property is for set the maximum number of records
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100");
        //Create Consumer:
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //Subscribe our consumer to a specific topics :
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    //create json parser
    private static final JsonParser jsonParser = new JsonParser();

    //method to create
    private static String extractIdFromTweet(String tweet){
        return jsonParser.parse(tweet).getAsJsonObject().get("id_str").getAsString();
    }

    //method to create elasticsearch client
    public static RestHighLevelClient createClient(){
        RestClientBuilder builder = RestClient.builder(new HttpHost("localhost", 9200, "http"));
        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    //main method
    public static void main(String[] args) throws IOException {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class);

        //create the elasticsearch client
        RestHighLevelClient client = createClient();

        //create the kafka consumer
        KafkaConsumer<String, String> consumer = createConsumer("Tweets-from-java");

        //start consuming
        while (true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            Integer recordsCount = records.count();
            if (recordsCount > 0){ logger.info("Received "+recordsCount+" records"); }

            BulkRequest bulkRequest = new BulkRequest();
            for(ConsumerRecord<String, String> record : records){
                String tweet = record.value();
                try {
                    //kafka generic ID "solution one"
                    //String kafkaGenericID = record.key() + "_" + record.value() + "_" +record.offset();

                    //tweet id "solution two"
                    String id = extractIdFromTweet(tweet);
                    //create the request that will send to elastic search (id as param to ignore insertion of duplicate records)
                    IndexRequest indexRequest = new IndexRequest("twitter", "tweets", id).source(tweet, XContentType.JSON);
                    bulkRequest.add(indexRequest); // add records into bulk request

                    //This is a long method to optimize our consumer we create the BulkRequest method
                    /*
                    IndexResponse indexResponse = client.index(indexRequest, RequestOptions.DEFAULT);
                    logger.info("id ===>  " + id );

                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    */
                }catch (NullPointerException e){
                    logger.warn("Skipping bad data : "+ tweet);
                }
            }
            if (recordsCount > 0){
                BulkResponse bulkResponse = client.bulk(bulkRequest, RequestOptions.DEFAULT);
                logger.info("Committing offsets ...");
                consumer.commitSync();
                logger.info("Offsets successfully committed");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
