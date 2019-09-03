package am.ik.blog.accesslogformatter.metric;

import am.ik.blog.accesslogformatter.JobData;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.core.io.Resource;
import org.springframework.http.converter.json.Jackson2ObjectMapperBuilder;

import java.io.FileOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public class FormatterTasklet implements Tasklet {

    private final Resource input;

    private final Resource output;

    private final ObjectMapper objectMapper;

    private final JobData jobData;

    public FormatterTasklet(Resource input, Resource output, JobData jobData) {
        this.input = input;
        this.output = output;
        this.objectMapper = Jackson2ObjectMapperBuilder.json()
            .indentOutput(true)
            .build();
        this.jobData = jobData;
    }


    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        final List<Map<String, ?>> result = this.objectMapper.readValue(this.input.getInputStream(), new TypeReference<>() {

        });

        if (!result.isEmpty()) {
            int size = ((List) result.get(0).get("values")).size();
            List<List<Object>> values = new ArrayList<>(size);
            result.forEach(map -> {
                final List vs = (List) map.get("values");
                if (values.isEmpty()) {
                    for (int i = 0; i < size; i++) {
                        final List v = (List) vs.get(i);
                        List<Object> value = new ArrayList<>();
                        value.add(v.get(0));
                        value.add(v.get(1));
                        values.add(value);
                    }
                } else {
                    for (int i = 0; i < size; i++) {
                        final List v = (List) vs.get(i);
                        values.get(i).set(1, (int) values.get(i).get(1) + (int) v.get(1));
                    }
                }
            });
            result.add(0, Map.of(
                "metric", Map.of("path", "total"),
                "values", values));
        }
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
