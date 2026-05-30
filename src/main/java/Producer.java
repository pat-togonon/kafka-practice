import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

import static org.apache.kafka.clients.producer.ProducerConfig.*;

public class Producer {

    public static void main(String[] args) {
        final Properties props = new Properties() {{
           put(BOOTSTRAP_SERVERS_CONFIG, "localhost:49703");
           put(KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
           put(VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
           put(ACKS_CONFIG, "all");
        }};

    }
}
