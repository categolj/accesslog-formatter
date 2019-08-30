package am.ik.blog.accesslogformatter.metric;

import am.ik.blog.accesslogformatter.BatchProps;
import am.ik.github.GitHubClient;
import am.ik.github.core.Committer;
import am.ik.github.core.Content;
import am.ik.github.repositories.contents.ContentsApi;
import am.ik.github.repositories.contents.ContentsRequest;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.util.StreamUtils;
import org.springframework.web.reactive.function.client.WebClient;

import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;

public class GithubUploadTasklet implements Tasklet {

    private final GitHubClient gitHubClient;

    private final BatchProps props;

    public GithubUploadTasklet(WebClient.Builder builder, BatchProps props) {
        this.gitHubClient = new GitHubClient(builder, props.getGithubAccessToken());
        this.props = props;
    }

    @Override
    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
        final ContentsApi.File file = this.gitHubClient.file("categolj", "accesslog", "metrics.json");
        final String promql = StreamUtils.copyToString(this.props.getPromqlResource().getInputStream(), StandardCharsets.UTF_8);
        file
            .get()
            .map(Content::getSha)
            .flatMap(sha -> file
                .update(ContentsRequest.Builder
                    .fromPlainText(promql)
                    .committer(new Committer("making-bot", "makingx+bot@gmail.com"))
                    .toUpdate("Metrics as of " + OffsetDateTime.now(), sha)))
            .block();
        return RepeatStatus.FINISHED;
    }
}
