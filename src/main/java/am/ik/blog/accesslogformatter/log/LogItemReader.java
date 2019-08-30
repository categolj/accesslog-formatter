package am.ik.blog.accesslogformatter.log;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import org.springframework.batch.item.ItemReader;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;

public class LogItemReader implements ItemReader<JsonNode> {

    private final LogSearcher logSearcher;

    private final BlockingDeque<JsonNode> deque = new LinkedBlockingDeque<>();

    private static final JsonNode POISON = NullNode.getInstance();

    public LogItemReader(LogSearcher logSearcher) {
        this.logSearcher = logSearcher;
        this.logSearcher.search("_all")
            .doOnNext(this.deque::addLast)
            .doOnTerminate(() -> this.deque.addLast(POISON))
            .subscribe();
    }

    @Override
    public JsonNode read() throws Exception {
        final JsonNode jsonNode = this.deque.pollFirst(1, TimeUnit.HOURS);
        if (jsonNode == POISON) {
            return null;
        }
        return jsonNode;
    }
}
