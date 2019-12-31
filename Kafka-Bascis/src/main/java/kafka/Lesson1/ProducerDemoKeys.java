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
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {

  public static void main(String[] args) throws ExecutionException, InterruptedException {
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


        for (int i=0; i<10; i++ ) {
              // create a producer record

              String topic = "first_topic";
              String value = "hello world " + Integer.toString(i);
              String key = "id_" + Integer.toString(i);

              ProducerRecord<String, String> record =
                  new ProducerRecord<String, String>(topic, key, value);

              logger.info("Key: " + key); // log the key
              // id_0 is going to partition 1
              // id_1 partition 0
              // id_2 partition 2
              // id_3 partition 0
              // id_4 partition 2
              // id_5 partition 2
              // id_6 partition 0
              // id_7 partition 2
              // id_8 partition 1
              // id_9 partition 2


              // send data - asynchronous
              producer.send(record, new Callback() {
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                          // executes every time a record is successfully sent or an exception is thrown
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
              }).get(); // block the .send() to make it synchronous - don't do this in production!
        }

        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
  }


}
