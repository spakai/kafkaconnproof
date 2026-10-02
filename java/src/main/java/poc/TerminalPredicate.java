package poc;

import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.predicates.Predicate;

/** Content predicate; Apache's built-in predicates do not inspect value fields. */
public final class TerminalPredicate<R extends ConnectRecord<R>> implements Predicate<R> {
    public static boolean terminal(Object type) {
        return "SUCCESS".equals(type) || "FAILED".equals(type);
    }
    @Override public boolean test(R record) {
        Object value = record.value();
        Object type = value instanceof Map ? ((Map<?, ?>) value).get("eventType")
                : value instanceof Struct ? ((Struct) value).get("eventType") : null;
        return terminal(type);
    }
    @Override public ConfigDef config() { return new ConfigDef(); }
    @Override public void configure(Map<String, ?> config) { }
    @Override public void close() { }
}
