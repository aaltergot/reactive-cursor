package rcursor.it;

import java.io.IOException;
import java.sql.Connection;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.apache.http.HttpHost;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.postgresql.ds.PGSimpleDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rcursor.BatchUpdate;
import rcursor.CursorContentsEmitter;
import rcursor.ElasticsearchBulkIndex;
import rcursor.Entity;
import rcursor.jdbc.ConnectionManager;
import rcursor.jdbc.PostgreSQLHelper;
import reactor.core.publisher.Flux;

import static org.junit.jupiter.api.Assertions.*;

/**
 * PostgresToElasticsearchIT.
 * PostgreSQL:
 * - host = localhost
 * - port is parsed from "rcursor.it.pg.port" system property or 5432 if not set
 * - username = postgres
 * - password = postgres
 * Elasticsearch:
 * - host = localhost
 * - port is parsed from "rcursor.it.es.port" system property or 9200 if not set
 * See also "enable-it" Maven profile.
 */
class PostgresToElasticsearchIT {
    private static final Logger logger = LoggerFactory.getLogger(PostgresToElasticsearchIT.class);

    private final Executor executor = Executors.newFixedThreadPool(5);
    private DataSource pgDs;
    private RestHighLevelClient esClient;

    @BeforeEach
    void beforeEach() {
        final PGSimpleDataSource pgsDs = new PGSimpleDataSource();
        pgsDs.setPortNumber(Util.getPostgresPort());
        pgsDs.setUser("postgres");
        pgsDs.setPassword("postgres");
        pgsDs.setPassword("postgres");
        pgsDs.setDatabaseName("postgres");
        this.pgDs = pgsDs;
        esClient = new RestHighLevelClient(RestClient.builder(
            new HttpHost("localhost", Util.getElasticsearchPort(), "http")
        ));
    }

    @AfterEach
    void afterAll() {
        try {
            esClient.close();
        } catch (IOException e) {
            logger.error("Failed to close Elasticsearch Client", e);
        }
    }

    @Test
    void pgToEs() throws Exception {
        final ConnectionManager conMgr = ConnectionManager.withTransaction(pgDs);
        final int count = 50000;
        populateEntities(conMgr, count);
        createIndex(esClient);
        Flux
            .create(CursorContentsEmitter.create(
                conMgr,
                PostgreSQLHelper.forwardOnlyQuery("select id from my_table"),
                Entity::mapEntity,
                executor
            ))
            .compose(ElasticsearchBulkIndex.create(
                esClient::bulk,
                BulkRequest::new,
                Entity::toIndexable
            ))
            .subscribe();
        final SearchRequest searchReq = new SearchRequest()
            .indices(Entity.ES_INDEX)
            .types(Entity.ES_TYPE)
            .source(
                new SearchSourceBuilder().query(QueryBuilders.matchAllQuery()).size(100)
            );

        for (int retriesLeft = 4; retriesLeft >= 0; retriesLeft--) {
            // Elasticsearch takes some time to index all the docs
            final SearchResponse entities = esClient.search(searchReq);
            if (retriesLeft == 0 || entities.getHits().getTotalHits() == count) {
                assertEquals(count, entities.getHits().getTotalHits());
                assertEquals(100, entities.getHits().getHits().length);
            } else {
                Thread.sleep(1000L);
            }
        }
    }

    @SuppressWarnings("SameParameterValue")
    private static void populateEntities(
        final ConnectionManager conMgr,
        final int count
    ) throws Exception {
        final Connection con = conMgr.get();
        Exception dbFailure = null;
        try {
            con.prepareStatement("drop table if exists my_table").executeUpdate();
            con.prepareStatement("create table my_table (id TEXT primary key)").executeUpdate();
            Entity.make(count)
                .compose(BatchUpdate.create(
                    ConnectionManager.withConnection(con),
                    c -> c.prepareStatement("insert into my_table values (?)"),
                    (ps, e) -> ps.setString(1, e.id)
                ))
                .subscribe();
        } catch (Exception e) {
            dbFailure = e;
        } finally {
            conMgr.dispose(con, dbFailure != null);
        }
        if (dbFailure !=null) {
            throw dbFailure;
        }
    }

    private static void createIndex(final RestHighLevelClient esClient) throws Exception {
        try {
            esClient.indices().delete(new DeleteIndexRequest(Entity.ES_INDEX));
        } catch (Exception ignore) {
        }
        final CreateIndexRequest req = new CreateIndexRequest();
        req.index(Entity.ES_INDEX).mapping(Entity.ES_TYPE, "test", "type=text");
        esClient.indices().create(req);
    }
}
