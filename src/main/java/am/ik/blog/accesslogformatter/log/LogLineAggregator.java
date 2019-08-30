package am.ik.blog.accesslogformatter.log;

import am.ik.blog.accesslogformatter.JobData;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.batch.item.file.transform.LineAggregator;

import java.time.OffsetDateTime;

public class LogLineAggregator implements LineAggregator<JsonNode> {

    private final JobData jobData;

    public LogLineAggregator(JobData jobData) {
        this.jobData = jobData;
    }

    @Override
    public String aggregate(JsonNode node) {
        final OffsetDateTime dateTime = OffsetDateTime.parse(node.get("_source").get("@timestamp").asText());
        final String path = node.get("_source").get("path").asText();
        final int second = dateTime.getSecond();
        final OffsetDateTime offsetDateTime = dateTime.withNano(0).withSecond((second % 2) * 30).withMinute(0);
        this.jobData.setTimestamp(offsetDateTime);
        return String.format("%s,%s", path, offsetDateTime);
    }
}
