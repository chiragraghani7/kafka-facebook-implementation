package com.javatechie.social.api;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.social.facebook.api.Facebook;
import org.springframework.social.facebook.api.Post;
import org.springframework.social.facebook.api.impl.FacebookTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

@Component
public class FacebookDemo {
    @Value("${facebook.access_token}")
    private String accessToken;
    static Logger logger = LoggerFactory.getLogger(FacebookDemo.class.getName());


    public String getFacebookObject(){
        Facebook facebook = new FacebookTemplate(accessToken);
        List<Post> feed = facebook.feedOperations().getFeed();
        KafkaProducer<String, String> producer = createKafkaProducer();
        String data="";
        for(Post post : feed){
            String json = "{\"id\": \"" + post.getId() + "\",\"message\": \"" +  post.getMessage() + "\",\"caption\": \"" + post.getCaption() + "\"}";
            logger.info(json);
//            JsonElement jsonTree = jsonParser.parse(json);
//            JsonObject jsonObject = jsonTree.getAsJsonObject();
            //data+="post from :"+" "+ post.getFrom()+" post caption:"+post.getCaption()+" post message:"+post.getMessage()+" post picture"+post.getPicture();

            //            logger.info("post from :"+p.getFrom());
//            logger.info("post message"+p.getMessage());
//            logger.info("post caption"+p.getCaption());
//            logger.info("post picture"+p.getPicture());
            //logger.info(jsonObject.toString());
            producer.send(new ProducerRecord<>("facebook_posts", null, json), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("Something bad happened", e);
                    }
                }
            });

            //data = "";
        }
        logger.info("post:"+feed.get(0).getFrom().getName());
        logger.info(String.valueOf(feed));
        logger.info("end of the post");
        return "hello";

    }

    public KafkaProducer<String, String> createKafkaProducer(){
        String bootstrapServers = "127.0.0.1:9092";

        // create Producer properties
        Properties properties = new Properties();

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create safe Producer
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5"); // kafka 2.0 >= 1.1 so we can keep this as 5. Use 1 otherwise.

        // high throughput producer (at the expense of a bit of latency and CPU usage)
//        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
//        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
//        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024)); // 32 KB batch size

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        return producer;
    }

    public static RestHighLevelClient createClient(){

        //////////////////////////
        /////////// IF YOU USE LOCAL ELASTICSEARCH
        //////////////////////////

        //  String hostname = "localhost";
        //  RestClientBuilder builder = RestClient.builder(new HttpHost(hostname,9200,"http"));


        //////////////////////////
        /////////// IF YOU USE BONSAI / HOSTED ELASTICSEARCH
        //////////////////////////

        // replace with your own credentials
        //https://:@
        String hostname = ""; // localhost or bonsai url
        String username = ""; // needed only for bonsai
        String password = ""; // needed only for bonsai

        // credentials provider help supply username and password
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
                        return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                    }
                });

        RestHighLevelClient client = new RestHighLevelClient(builder);
        return client;
    }

    public void createConsumer(String topic){

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch1";
        logger.info("inside function creteConsumer"+topic);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test");
        props.put("enable.auto.commit", "false");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // create consumer configs
//        Properties properties = new Properties();
//        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
//        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
//        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // disable auto commit of offsets
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "100"); // disable auto commit of offsets

        logger.info("after setting properties");
        BulkRequest bulkRequest = new BulkRequest();

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
        logger.info("after declaration");
        consumer.subscribe(Arrays.asList(topic));

        while(true){
            ConsumerRecords<String, String> records =
                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

            for (ConsumerRecord<String, String> record : records){
                logger.info("Key: " + record.key() + ", Value: " + record.value());
                logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                String id = extractIdFromTweet(record.value());
                logger.info("unique id:"+id);
                //IndexRequest indexRequest = new IndexRequest("facebook").source(record.value(),XContentType.JSON);

                    IndexRequest indexRequest = new IndexRequest("facebook")
                            .source(record.value(), XContentType.JSON)
                            .id(id);
                    indexRequest.routing("posts");
                bulkRequest.add(indexRequest);
            }
        }


        //logger.info("consumer"+consumer);
        //return consumer;

    }



    public String elasticSearch() throws IOException {
        RestHighLevelClient client = createClient();

        //KafkaConsumer<String, String> consumer =
            createConsumer("facebook_posts");

//        while (true) {
//            ConsumerRecords<String, String> records =
//                    consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0
//
//            Integer recordCount = records.count();
//            logger.info("Received " + recordCount + " records");
//            logger.info("meri gand fatt gyii");
//
//            BulkRequest bulkRequest = new BulkRequest();
//
//            for (ConsumerRecord<String, String> record : records) {
//                try {
//                    String id = extractIdFromTweet(record.value());
//                    logger.info("extract id:"+id);
//
//
////                    IndexRequest indexRequest = new IndexRequest(
////                            "facebook",
////                            "posts",
////                            id // this is to make our consumer idempotent
////                    ).source(record.value(), XContentType.JSON); // this is to make our consumer idempotent
//
//                    IndexRequest indexRequest = new IndexRequest("posts")
//                            .source(record.value(), XContentType.JSON)
//                            .id(id);
//
//                    bulkRequest.add(indexRequest); // we add to our bulk request (takes no time)
//                } catch (NullPointerException e) {
//                    logger.warn("skipping bad data: " + record.value());
//                }
//
//            }
//
//            if (recordCount > 0) {
//                BulkResponse bulkItemResponses = client.bulk(bulkRequest, RequestOptions.DEFAULT);
//                logger.info("Committing offsets...");
//                consumer.commitSync();
//                logger.info("Offsets have been committed");
//                try {
//                    Thread.sleep(1000);
//                } catch (InterruptedException e) {
//                    e.printStackTrace();
//                }
//            }
            return "hello";
        //}
    }
        private static JsonParser jsonParser = new JsonParser();

        private static String extractIdFromTweet(String tweetJson){
            // gson library
            JsonElement jsonTree = jsonParser.parse(tweetJson);
            JsonObject jsonObject = jsonTree.getAsJsonObject();
            if (jsonTree.isJsonNull() || !jsonObject.isJsonObject()){
               return "1";
            }else {
                return jsonParser.parse(tweetJson)
                        .getAsJsonObject()
                        .get("id")
                        .getAsString();
            }
        }
}
