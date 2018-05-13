package rcursor.elasticsearch;

import java.io.IOException;
import org.apache.http.Header;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;

/**
 * BulkAction is a function used by {@link rcursor.ElasticsearchBulkIndex} to send BulkRequest.
 * This eliminates dependency on {@link org.elasticsearch.client.RestHighLevelClient} and
 * thus makes code easier to test.
 * Usually implemented as restHighLevelClient::bulk.
 */
@FunctionalInterface
public interface BulkAction {
    BulkResponse bulk(BulkRequest req, Header... headers) throws IOException;
}
