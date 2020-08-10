package kafka.tutorial1;

import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class DemoApp {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(DemoApp.class);

        String bootstrapServers = "127.0.0.1:9092";
        // producer proteries
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create the producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);
        for(int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello_world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            logger.info("Key: " + key);
            // create a producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, key, value);
            // send data - asynchronous
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // executes every time a record is successfully sent
                    if(exception == null) {
                        logger.info("Received new metadata. \n" +
                                "Topic:" + metadata.topic() + "\n" +
                                "Partition:" + metadata.partition() + "\n" +
                                "Offset:" + metadata.offset() + "\n" +
                                "Timestamp" + metadata.timestamp());
                    } else {
                        logger.error("Error while producing", exception);
                    }
                }
            }).get(); // block the .send() to make it synchronous
        }
        // flush data
        producer.flush();
        // flush and close producer
        producer.close();
    }
}
