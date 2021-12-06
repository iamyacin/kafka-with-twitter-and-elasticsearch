package com.github.iamyacin.kafka.twitter;

import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    //Constructor:
    public TwitterProducer(){}

    //Client Creation method :
    public Client createClient(BlockingQueue<String> msgQ){
        // Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth)
        Hosts host = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint endPoint = new StatusesFilterEndpoint();
        // Optional: set up some followings and track terms
        //List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("bitcoin");
        //endPoint.followings(followings);
        endPoint.trackTerms(terms);

        // These secrets should be read from a config file
        Authentication auth = new OAuth1(TwitterCredentials.CONSUMER_KEY,
                TwitterCredentials.CONSUMER_SECRET,
                TwitterCredentials.TOKEN,
                TwitterCredentials.TOKEN_SECRET);

        ClientBuilder builder = new ClientBuilder()
                .name("Client-01")  // optional: mainly for the logs
                .hosts(host)
                .authentication(auth)
                .endpoint(endPoint)
                .processor(new StringDelimitedProcessor(msgQ));
        //.eventMessageQueue(eventQueue); // optional: use this if you want to process client events
        return builder.build();
    }

    public KafkaProducer<String, String> createKafkaProducer(){
        //Define the bootstrapServers variable :
        String bootstrapServers = "localhost:9092";
        // Create the producer properties :
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create safe producer :
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");

        //Add high throughput producer (at the expense of a bit of latency and CPU usage)
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32KB

        // Create the producer :
        return new KafkaProducer<>(properties);
    }

    //Run method:
    public void run() throws InterruptedException {
        BlockingQueue<String> msgQ = new LinkedBlockingQueue<>(1000);
        Client client = createClient(msgQ);
        client.connect();

        KafkaProducer<String, String> producer = createKafkaProducer();

        // on a different thread, or multiple different threads....
        while (!client.isDone()) {
            String msg;
            msg = msgQ.poll(5, TimeUnit.SECONDS);
            if (msg!=null){
                System.out.println(msg);
                producer.send(new ProducerRecord<>("Tweets-from-java", null, msg), (recordMetadata, e) -> {
                    if (e != null){
                        System.out.println(e.toString());
                    }
                });
            }
        }
    }
    public static void main(String[] args) throws InterruptedException {
        new TwitterProducer().run();
    }
}
