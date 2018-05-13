package rcursor;

import java.util.function.Supplier;
import org.apache.http.Header;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import rcursor.elasticsearch.BulkAction;
import rcursor.elasticsearch.IndexableItem;
import rcursor.elasticsearch.IndexableMapping;
import reactor.test.StepVerifier;

import static org.mockito.Mockito.*;

/**
 * ElasticsearchBulkIndexTest.
 */
class ElasticsearchBulkIndexTest {

    private BulkAction bulkAction;
    private Supplier<BulkRequest> requestSupplier;
    private IndexableMapping<Entity> mapping;

    @BeforeEach
    @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
    void beforeEach() {
        bulkAction = spy(new BulkAction() {
            @Override
            public BulkResponse bulk(final BulkRequest req, final Header... headers) {
                return new BulkResponse(new BulkItemResponse[0], 100L);
            }
        });
        requestSupplier = spy(new Supplier<BulkRequest>() {
            @Override public BulkRequest get() {
                return new BulkRequest();
            }
        });
        mapping = spy(new IndexableMapping<Entity>() {
            @Override public IndexableItem toIndexable(final Entity item) throws Exception {
                return Entity.toIndexable(item);
            }
        });
    }

    @Test
    void success() throws Exception {
        final ElasticsearchBulkIndex<Entity> esBulk =
            new ElasticsearchBulkIndex<>(bulkAction, requestSupplier, mapping, 2);

        StepVerifier.create(Entity.make(5).compose(esBulk))
            .expectSubscription()
            .expectNextCount(5)
            .verifyComplete();

        verify(bulkAction, new Times(3)).bulk(any(), any());
        verify(requestSupplier, new Times(3)).get();
        verify(mapping, new Times(5)).toIndexable(any());
        verifyNoMoreInteractions(bulkAction, requestSupplier, mapping);
    }

    @Test
    void bulkFailure() throws Exception {
        when(bulkAction.bulk(any(), any())).thenThrow(new RuntimeException("Oops"));
        final ElasticsearchBulkIndex<Entity> esBulk =
            ElasticsearchBulkIndex.create(bulkAction, requestSupplier, mapping);

        StepVerifier.create(Entity.make(5).compose(esBulk))
            .expectSubscription()
            .verifyError();

        verify(bulkAction, new Times(1)).bulk(any(), any());
        verify(requestSupplier, new Times(1)).get();
        verify(mapping, new Times(5)).toIndexable(any());
        verifyNoMoreInteractions(bulkAction, requestSupplier, mapping);
    }

    @Test
    void bulkHasFailures() throws Exception {
        when(bulkAction.bulk(any(), any())).thenReturn(new BulkResponse(
            new BulkItemResponse[] {
                new BulkItemResponse(
                    1,
                    DocWriteRequest.OpType.INDEX,
                    new BulkItemResponse.Failure(
                        "entities",
                        "entity",
                        "id0",
                        new RuntimeException("Oops"),
                        true
                    )
                )
            },
            100L
        ));
        final ElasticsearchBulkIndex<Entity> esBulk =
            ElasticsearchBulkIndex.create(bulkAction, requestSupplier, mapping);

        StepVerifier.create(Entity.make(5).compose(esBulk))
            .expectSubscription()
            .verifyError();

        verify(bulkAction, new Times(1)).bulk(any(), any());
        verify(requestSupplier, new Times(1)).get();
        verify(mapping, new Times(5)).toIndexable(any());
        verifyNoMoreInteractions(bulkAction, requestSupplier, mapping);
    }
}