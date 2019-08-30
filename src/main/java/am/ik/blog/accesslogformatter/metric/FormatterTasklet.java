package am.ik.blog.accesslogformatter.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.Resource;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.io.FileOutputStream;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FormatterTasklet implements Tasklet {

    private final Resource input;

    private final Resource output;

    private final ObjectMapper objectMapper;

    public FormatterTasklet(Resource input, Resource output) {
        this.input = input;
        this.output = output;
        this.objectMapper = Jackson2ObjectMapperBuilder.json()
            .indentOutput(true)
            .build();
    }


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        final List result = this.objectMapper.readValue(this.input.getInputStream(), List.class);
        final Map<String, Object> response = Map.of(
            "status", "success"
            , "data", Map.of(
                "resultType", "matrix",
                "result", result
            ));
        try (FileOutputStream stream = new FileOutputStream(this.output.getFile())) {
            this.objectMapper.writeValue(stream, new TreeMap<>(response));
        }
        return RepeatStatus.FINISHED;
    }
}
