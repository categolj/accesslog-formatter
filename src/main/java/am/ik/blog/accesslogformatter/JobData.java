package am.ik.blog.accesslogformatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.stream.Stream;

public class JobData {

    private final Logger log = LoggerFactory.getLogger(JobData.class);

    private OffsetDateTime start = OffsetDateTime.MAX;

    private OffsetDateTime end = OffsetDateTime.MIN;


    public void setTimestamp(OffsetDateTime offsetDateTime) {
        if (offsetDateTime.isAfter(end)) {
            this.end = offsetDateTime;
        }
        if (offsetDateTime.isBefore(start)) {
            this.start = offsetDateTime;
        }
    }

    public Stream<Instant> timestamps() {
        return Stream.iterate(this.start,
            offsetDateTime -> offsetDateTime.isBefore(end),
            x -> x.plusHours(1))
            .map(OffsetDateTime::toInstant);
    }
}
