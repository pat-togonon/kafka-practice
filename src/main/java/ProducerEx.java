import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class ProducerEx {

    public static void main(String[] args) {
        final Properties props = new Properties() {{
           put(BOOTSTRAP_SERVERS_CONFIG, "localhost:49703");
           put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
           put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
           put(ACKS_CONFIG, "all");
        }};

        final String topic = "purchases";

        String[] users = { "pat94", "mela", "chess16", "laine", "ferdz" };
        String[] items = { "book", "t-shirt", "food-tray", "keyboard", "dress" };

        try (final Producer<String, String> producer = new KafkaProducer<>(props)) {
            final Random rnd = new Random();
            final int numMessages = 10;
            for (int i = 0; i < numMessages; i++) {
                String user = users[rnd.nextInt(users.length)];
                String item = items[rnd.nextInt(items.length)];

                producer.send(
                        new ProducerRecord<>(topic, user, item),
                        (event, ex) -> {
                            if (ex != null) {
                                ex.printStackTrace();
                            }
                            else {
                                System.out.printf("Produced event to topic $s: key = %-10s value = %s%n", topic, user, item);
                            }
                        }
                );
            }
            System.out.printf("%s events were produced to topic %s%n", numMessages, topic);
        }

    }
}
