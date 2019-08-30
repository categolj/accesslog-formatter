package am.ik.blog.accesslogformatter.log;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.batch.item.ItemProcessor;

public class LogItemProcessor implements ItemProcessor<JsonNode, JsonNode> {

    @Override
    public JsonNode process(JsonNode node) throws Exception {
        if (!node.get("_source").get("path").asText("").startsWith("/entries/")) {
            return null;
        }
        if (node.get("_source").get("useragent").asText("").contains("Prerender")) {
            return null;
        }
        return node;
    }
}
