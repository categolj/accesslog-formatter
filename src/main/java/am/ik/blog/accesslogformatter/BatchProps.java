package am.ik.blog.accesslogformatter;

import am.ik.github.AccessToken;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.core.io.Resource;

@ConfigurationProperties(prefix = "formatter")
public class BatchProps {

    private String elasticsearchUrl;

    private AccessToken githubAccessToken;

    private Resource dumpResource;

    private Resource sortResource;

    private Resource jsonResource;

    private Resource promqlResource;

    public Resource getDumpResource() {
        return dumpResource;
    }

    public String getElasticsearchUrl() {
        return elasticsearchUrl;
    }

    public AccessToken getGithubAccessToken() {
        return githubAccessToken;
    }

    public void setDumpResource(Resource dumpResource) {
        this.dumpResource = dumpResource;
    }

    public Resource getSortResource() {
        return sortResource;
    }

    public void setElasticsearchUrl(String elasticsearchUrl) {
        this.elasticsearchUrl = elasticsearchUrl;
    }

    public void setGithubAccessToken(AccessToken githubAccessToken) {
        this.githubAccessToken = githubAccessToken;
    }

    public void setSortResource(Resource sortResource) {
        this.sortResource = sortResource;
    }

    public Resource getJsonResource() {
        return jsonResource;
    }

    public void setJsonResource(Resource jsonResource) {
        this.jsonResource = jsonResource;
    }

    public Resource getPromqlResource() {
        return promqlResource;
    }

    public void setPromqlResource(Resource promqlResource) {
        this.promqlResource = promqlResource;
    }
}
