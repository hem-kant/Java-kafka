package com.github.hemkant.kafka.Lesson1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerDemo {

  public static void main(String[] args) {
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
        producer.send(record);
        producer.flush();
        producer.close(); // close the producer
  }

}
