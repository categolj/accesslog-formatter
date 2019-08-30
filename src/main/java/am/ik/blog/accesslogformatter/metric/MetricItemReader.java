package am.ik.blog.accesslogformatter.metric;

import am.ik.blog.accesslogformatter.JobData;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.NonTransientResourceException;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.UnexpectedInputException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.support.SingleItemPeekableItemReader;
import org.springframework.core.io.Resource;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.Objects;

public class MetricItemReader implements ItemStreamReader<Metric> {

    private final SingleItemPeekableItemReader<String> itemReader;

    private final JobData jobData;

    public MetricItemReader(Resource input, JobData jobData) {
        this.jobData = jobData;
        this.itemReader = new SingleItemPeekableItemReader<>();
        final FlatFileItemReader<String> delegate = new FlatFileItemReader<>();
        delegate.setResource(input);
        delegate.setLineMapper((s, i) -> s);
        this.itemReader.setDelegate(delegate);
    }


    @Override
    public Metric read() throws Exception, UnexpectedInputException, ParseException, NonTransientResourceException {
        String line = this.itemReader.read();
        if (line == null) {
            return null;
        }
        final Tuple2<String, Instant> split = this.split(line);
        final String path = split.getT1();
        final Metric metric = new Metric(path, this.jobData.timestamps());
        Instant timestamp = split.getT2();
        String nextLine = this.itemReader.peek();
        while (nextLine != null) {
            final Tuple2<String, Instant> nextSplit = this.split(nextLine);
            if (!Objects.equals(path, nextSplit.getT1())) {
                metric.increment(timestamp);
                break;
            }
            if (timestamp.equals(nextSplit.getT2())) {
                metric.increment(timestamp);
            } else {
                timestamp = nextSplit.getT2();
            }
            this.itemReader.read();
            nextLine = this.itemReader.peek();
        }
        return metric;
    }

    private Tuple2<String, Instant> split(String line) {
        final String[] split = line.split(",");
        return Tuples.of(split[0], OffsetDateTime.parse(split[1]).toInstant());
    }


    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        this.itemReader.open(executionContext);
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
        this.itemReader.update(executionContext);
    }

    @Override
    public void close() throws ItemStreamException {
        this.itemReader.close();
    }
}
