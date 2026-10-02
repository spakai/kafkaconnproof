package poc;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Duration;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

/** At-least-once projection: acknowledge output before committing input. */
public final class Projection {
    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("BOOTSTRAP_SERVERS", "kafka:9092");
        Properties c = new Properties();
        c.put("bootstrap.servers", bootstrap);
        c.put("group.id", "rerate-terminal-projection");
        c.put("enable.auto.commit", "false");
        c.put("auto.offset.reset", "earliest");
        c.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        c.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        Properties p = new Properties();
        p.put("bootstrap.servers", bootstrap);
        p.put("acks", "all"); p.put("enable.idempotence", "true");
        p.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        p.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        ObjectMapper json = new ObjectMapper();
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(c);
             KafkaProducer<String, String> producer = new KafkaProducer<>(p)) {
            consumer.subscribe(List.of("rerate-edr"));
            while (!Thread.currentThread().isInterrupted()) {
                var records = consumer.poll(Duration.ofSeconds(1));
                for (var record : records) {
                    Map<?, ?> event = json.readValue(record.value(), Map.class);
                    if (!"SUCCESS".equals(event.get("eventType")) && !"FAILED".equals(event.get("eventType"))) continue;
                    Map<String, Object> terminal = new LinkedHashMap<>();
                    for (String field : List.of("subscriberId", "jobId", "txnId"))
                        terminal.put(field, event.get(field));
                    terminal.put("status", event.get("eventType"));
                    // Stable on replay. Fixture timestamps are unique per subscriber.
                    terminal.put("ingestionTime", java.time.Instant.ofEpochMilli(record.timestamp()).toString());
                    producer.send(new ProducerRecord<>("rerate-edr-terminal", null,
                        record.timestamp(), record.key(), json.writeValueAsString(terminal))).get();
                }
                if (!records.isEmpty()) consumer.commitSync();
            }
        }
    }
}
