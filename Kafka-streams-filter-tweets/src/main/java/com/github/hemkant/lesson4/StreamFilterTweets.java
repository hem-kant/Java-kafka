package com.github.hemkant.lesson4;

import com.google.gson.JsonParser;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class StreamFilterTweets {

  public static void main(String[] args) {
    System.out.println("Filter Tweets");
     // Create properties
    Logger logger = LoggerFactory.getLogger(StreamFilterTweets.class.getName());

    String bootstrapServers = "127.0.0.1:9092";
    String groupId = "demo-kafka-streams";
    String topic = "first_topic";

    // create consumer configs
    Properties properties = new Properties();
    properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    properties.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());

    // Create a topology
    StreamsBuilder streamsBuilder = new StreamsBuilder();
    KStream<String,String> inputTopic= streamsBuilder.stream("twitter_tweets");
    KStream<String,String> filterStream = inputTopic.filter(
        (k,jsonTweet)->  extractUserFollowersFromTweet(jsonTweet)>1000
          // Filter for tweets which has a user over 1K

    );
    filterStream.to("important_tweets");
    // build the topology
    KafkaStreams kafkaStreams = new KafkaStreams(streamsBuilder.build(),properties);


    // start our stream app

    kafkaStreams.start();
  }
  private static JsonParser jsonParser = new JsonParser();
  private static Integer extractUserFollowersFromTweet(String tweetJson){
    // gson library
    return jsonParser.parse(tweetJson)
        .getAsJsonObject()
        .get("user")
        .getAsJsonObject()
        .get("followers_count")
        .getAsInt();
  }
}
