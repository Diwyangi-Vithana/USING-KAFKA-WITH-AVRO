package EHR;

import com.example.Patient;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        Properties properties = new Properties();
        // normal producer
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("acks", "1");
        properties.setProperty("retries", "10");

        // avro part
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");

        KafkaProducer<String, Patient> kafkaProducer = new KafkaProducer<String, Patient>(properties);

        String topic = "patient-avro";

        Patient patient = Patient.newBuilder()
                .setFirstName("Diwyangi")
                .setLastName("Vithana")
                .setAge(23)
                .setHeight(178f)
                .setWeight(60f)
                .build();

        ProducerRecord<String, Patient> producerRecord = new ProducerRecord<String, Patient>(
                topic, patient
        );

        kafkaProducer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    System.out.println("Success!");
                    System.out.println(recordMetadata.toString());
                } else {
                    e.printStackTrace();
                }

            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();

    }
}
