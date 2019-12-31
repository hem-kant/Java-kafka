package com.github.hemkant.kafka.Lesson1;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.Properties;

public class ProducerDemoWithCallback {

  public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);
        System.out.println("Kafka");

        String bootstrapServers = "127.0.0.1:9092";
        // step 1 create a producer properties
        // https://kafka.apache.org/documentation/#producerconfigs refer KAFKA docs
        Properties properties = new Properties();

        // we can use Kafka producer config
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers); // kafka address
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // old way of defining the properties
        properties.setProperty("value.serializer",StringSerializer.class.getName());
        //END

        // step 2 create a producer
        KafkaProducer<String,String> producer = new KafkaProducer<String, String>(properties);
        //END

        // step 3 create a producer record
        ProducerRecord<String, String> record =
            new ProducerRecord<String, String>("first_topic","hellow world");
        // END

        // step 4 send data -- asynchronous
        producer.send(record, new Callback() {
              public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // execute evertime records successfully send or exception
                    if (e == null) {
                          // the record was successfully sent
                          logger.info("Received new metadata. \n" +
                              "Topic:" + recordMetadata.topic() + "\n" +
                              "Partition: " + recordMetadata.partition() + "\n" +
                              "Offset: " + recordMetadata.offset() + "\n" +
                              "Timestamp: " + recordMetadata.timestamp());
                    } else {
                          logger.error("Error while producing", e);
                    }
              }
        });
        producer.flush();
        producer.close(); // close the producer
  }

}
