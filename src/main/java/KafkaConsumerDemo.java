import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();

        // Kafka bootstrap server
        properties.setProperty("bootstrap.servers", "192.168.99.100:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());


        // Set Property of Group id
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "false");
//        properties.setProperty("auto.commit.interval.ms", "1000");
        properties.setProperty("auto.offset.reset", "earliest");


        // Create Kafka Consumer
        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<String, String>(properties);
        kafkaConsumer.subscribe(Arrays.asList("second_topic"));


        // Poll kafka consumer and get data
        while (true) {
            ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(100);

            // Looking for content
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
/*                consumerRecord.value();
                consumerRecord.key();
                consumerRecord.offset();
                consumerRecord.partition();
                consumerRecord.topic();
                consumerRecord.timestamp();*/

                System.out.println("Partition " + consumerRecord.partition() +
                                   ", Offset: " + consumerRecord.offset() +
                                   ", Key: " + consumerRecord.key() +
                                   ", Value: " + consumerRecord.value());


            }


            // Commit Trigger
            kafkaConsumer.commitSync();

        }

    }
}
