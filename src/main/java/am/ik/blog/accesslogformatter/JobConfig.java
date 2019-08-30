package am.ik.blog.accesslogformatter;

import am.ik.blog.accesslogformatter.log.LogItemProcessor;
import am.ik.blog.accesslogformatter.log.LogItemReader;
import am.ik.blog.accesslogformatter.log.LogLineAggregator;
import am.ik.blog.accesslogformatter.log.LogSearcher;
import am.ik.blog.accesslogformatter.metric.FormatterTasklet;
import am.ik.blog.accesslogformatter.metric.GithubUploadTasklet;
import am.ik.blog.accesslogformatter.metric.Metric;
import am.ik.blog.accesslogformatter.metric.MetricItemReader;
import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.listener.ItemListenerSupport;
import org.springframework.batch.core.step.tasklet.SystemCommandTasklet;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.WebClient;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

@Configuration
@EnableBatchProcessing
public class JobConfig {

    private final JobBuilderFactory jobBuilderFactory;

    private final StepBuilderFactory stepBuilderFactory;

    private final BatchProps props;


    private final ItemListenerSupport<JsonNode, JsonNode> itemListenerSupport = new ItemListenerSupport<>() {

        @Override
        public void onReadError(Exception ex) {
            LoggerFactory.getLogger("error").error("Encountered error on read", ex);
        }

        @Override
        public void onWriteError(Exception ex, List<? extends JsonNode> item) {
            LoggerFactory.getLogger("error").error("Encountered error on write", ex);
        }
    };

    public JobConfig(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, BatchProps props) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.props = props;
    }

    @Bean
    @JobScope
    public JobData jobData() {
        return new JobData();
    }

    @Bean
    public Step dump(LogSearcher logSearcher) {
        return stepBuilderFactory.get("dump") //
            .<JsonNode, JsonNode>chunk(200) //
            .reader(new LogItemReader(logSearcher)) //
            .processor(new LogItemProcessor()) //
            .writer(new FlatFileItemWriterBuilder<JsonNode>()
                .name("itemWriter")
                .resource(this.props.getDumpResource())
                .lineAggregator(new LogLineAggregator(this.jobData()))
                .build())
            .listener((Object) itemListenerSupport)
            .build();
    }

    @Bean
    public Step sort() throws Exception {
        final SystemCommandTasklet systemCommandTasklet = new SystemCommandTasklet();
        final String command = String.format("sort -o %s %s", this.props.getSortResource().getFile().getAbsolutePath(), this.props.getDumpResource().getFile().getAbsolutePath());
        systemCommandTasklet.setCommand(command);
        systemCommandTasklet.setTimeout(120_000);
        return stepBuilderFactory.get("sort") //
            .tasklet(systemCommandTasklet)
            .listener(itemListenerSupport)
            .build();
    }

    @Bean
    @JobScope
    public Step formatJson() {
        return stepBuilderFactory.get("formatJson") //
            .<Metric, Map<String, ?>>chunk(100)
            .reader(new MetricItemReader(this.props.getSortResource(), this.jobData())) //
            .processor((Function<? super Metric, ? extends Map<String, ?>>) Metric::format) //
            .writer(new JsonFileItemWriterBuilder<>()
                .name("resultWriter")
                .resource(this.props.getJsonResource())
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .build())
            .listener(itemListenerSupport)
            .build();
    }

    @Bean
    public Step formatPromQL() {
        return stepBuilderFactory.get("formatPromQL") //
            .tasklet(new FormatterTasklet(this.props.getJsonResource(), this.props.getPromqlResource()))
            .listener(itemListenerSupport)
            .build();
    }

    @Bean
    public Step githubUpload(WebClient.Builder builder) {
        return stepBuilderFactory.get("githubUpload") //
            .tasklet(new GithubUploadTasklet(builder, this.props))
            .listener(itemListenerSupport)
            .build();
    }

    @Bean
    public Step cleanup() throws Exception {
        final SystemCommandTasklet systemCommandTasklet = new SystemCommandTasklet();
        final String command = String.format("rm -f %s %s %s",
            this.props.getDumpResource().getFile().getAbsolutePath(),
            this.props.getSortResource().getFile().getAbsolutePath(),
            this.props.getJsonResource().getFile().getAbsolutePath());
        systemCommandTasklet.setCommand(command);
        systemCommandTasklet.setTimeout(120_000);
        return stepBuilderFactory.get("cleanup") //
            .tasklet(systemCommandTasklet)
            .listener(itemListenerSupport)
            .build();
    }

    @Bean
    public Job job() throws Exception {
        return this.jobBuilderFactory.get("job") //
            .incrementer(new RunIdIncrementer()) //
            .start(this.dump(null)) //
            .next(this.sort())
            .next(this.formatJson())
            .next(this.formatPromQL())
            .next(this.githubUpload(null))
            .next(this.cleanup())
            .build();
    }
}
