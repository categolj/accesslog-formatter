package am.ik.blog.accesslogformatter.metric;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Metric {

    private final String path;

    private final Map<Instant, Integer> valueMap;

    public Metric(String path, Stream<Instant> timestamps) {
        this.path = path;
        this.valueMap = timestamps.collect(Collectors.toMap(x -> x, x -> 0, (x, y) -> x, LinkedHashMap::new));
    }

    public void increment(Instant timestamp) {
        this.valueMap.computeIfPresent(timestamp, (t, v) -> v + 1);
    }

    public Map<String, ?> format() {
        return Map.of(
            "metric", Map.of("path", this.path),
            "values", this.valueMap.entrySet().stream().map(e -> List.of(e.getKey().getEpochSecond(), String.valueOf(e.getValue()))).collect(Collectors.toList()));
    }
}
