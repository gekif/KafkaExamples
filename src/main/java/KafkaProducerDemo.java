import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {

    public static void main(String[] args) {
        Properties properties = new Properties();

        // Kafka bootstrap server
        properties.setProperty("bootstrap.servers", "192.168.99.100:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        // Producer acks
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "3");
        properties.setProperty("linger.ms", "1");


        // Launch Kafka Producer
        Producer<String, String> producer = new org.apache.kafka.clients.producer.KafkaProducer<String, String>(properties);




        // Create producer records
/*        ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>
//                ("second_topic", "3", "message test");
                ("second_topic", "3", "another test");*/


        // Using For Loop To Create Producer Records
        for (int key = 0; key < 10; key++) {
            ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>
                    ("second_topic", Integer.toString(key), "message that has key: " + Integer.toString(key));

            producer.send(producerRecord);
        }

        System.out.println("Run Successfull");


        // Send The Data
//        producer.send(producerRecord);



        // Close The Producer
        producer.close();


    }
}
