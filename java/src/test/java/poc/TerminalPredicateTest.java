package poc;
import java.util.Map;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.transforms.Filter;
import org.apache.kafka.connect.transforms.RegexRouter;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
class TerminalPredicateTest {
    @Test void lifecycleAndRouting() {
        var predicate = new TerminalPredicate<SinkRecord>();
        var router = new RegexRouter<SinkRecord>();
        router.configure(Map.of("regex", "^rerate-edr$", "replacement", "rerate-edr-terminal-route"));
        var filter = new Filter<SinkRecord>();
        for (String type : new String[]{"SCHEDULED", "ATTEMPT", "OPERATION", "SUCCESS", "FAILED"}) {
            var input = new SinkRecord("rerate-edr", 0, null, "SUB001", null, Map.of("eventType", type), 7);
            boolean terminal = type.equals("SUCCESS") || type.equals("FAILED");
            assertEquals(terminal, predicate.test(input));
            var routed = terminal ? router.apply(input) : input;
            assertEquals(terminal ? "rerate-edr-terminal-route" : "rerate-edr", routed.topic());
            assertEquals(input.value(), routed.value());
            assertEquals(7, routed.kafkaOffset());
            assertNull(filter.apply(input));
        }
        assertFalse(predicate.test(new SinkRecord("rerate-edr", 0, null, null, null, null, 8)));
        router.close();
    }
}
