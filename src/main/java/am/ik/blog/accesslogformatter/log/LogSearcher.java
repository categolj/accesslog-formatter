package am.ik.blog.accesslogformatter.log;

import am.ik.blog.accesslogformatter.BatchProps;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.springframework.http.HttpMethod.GET;

@Component
public class LogSearcher {

    private final WebClient webClient;

    private static final int size = 200;

    public LogSearcher(WebClient.Builder builder, BatchProps props) {
        this.webClient = builder
            .baseUrl(props.getElasticsearchUrl())
            .build();
    }

    public Flux<JsonNode> search(String doc) {
        final Map<String, ?> query = Map.of("query",
            Map.of("bool",
                Map.of("must",
                    List.of(
                        Map.of("match_phrase",
                            Map.of("tags",
                                Map.of("query", "spring_boot"))),
                        Map.of("match_phrase",
                            Map.of("host",
                                Map.of("query", "blog-api.ik.am"))),
                        Map.of("match_phrase",
                            Map.of("crawler",
                                Map.of("query", "false"))),
                        Map.of("bool",
                            Map.of("should",
                                List.of(
                                    Map.of("match_phrase",
                                        Map.of("status", "200")),
                                    Map.of("match_phrase",
                                        Map.of("status", "304"))),
                                "minimum_should_match", 1))))));
        final Mono<Long> count = this.webClient.method(GET)
            .uri(b -> b
                .path("/{doc}/_count")
                .build(doc))
            .contentType(MediaType.APPLICATION_JSON)
            .syncBody(query)
            .retrieve()
            .bodyToMono(JsonNode.class)
            .map(res -> res.get("count").asLong())
            .log("count");
        return count.flatMapMany(c -> {
            int n = (int) (c / size) + 1;
            return Flux.range(0, n)
                .flatMap(page -> {
                    int from = page * size;
                    return this.webClient.method(GET)
                        .uri(b -> b
                            .path("/{doc}/_search")
                            .queryParam("size", size)
                            .queryParam("from", from)
                            .build(doc))
                        .contentType(MediaType.APPLICATION_JSON)
                        .syncBody(query)
                        .retrieve()
                        .bodyToMono(JsonNode.class)
                        .flatMapMany(node -> Flux.fromStream(StreamSupport.stream(node.get("hits").get("hits").spliterator(), false)));
                }, 32);
        });
    }
}
