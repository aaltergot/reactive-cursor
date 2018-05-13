package rcursor;

import java.io.IOException;
import java.util.function.Function;
import java.util.function.Supplier;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.reactivestreams.Publisher;
import rcursor.elasticsearch.BulkAction;
import rcursor.elasticsearch.IndexableItem;
import rcursor.elasticsearch.IndexableMapping;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * ElasticsearchBulkIndex.
 */
public class ElasticsearchBulkIndex<T> implements Function<Flux<T>, Publisher<T>> {

    private static final int DEFAULT_BULT_SIZE = 1024;

    private final BulkAction bulkAction;
    private final Supplier<BulkRequest> requestSupplier;
    private final IndexableMapping<T> mapping;
    private final int bulkSize;

    public ElasticsearchBulkIndex(
        final BulkAction bulkAction,
        final Supplier<BulkRequest> requestSupplier,
        final IndexableMapping<T> mapping,
        final int bulkSize
    ) {
        this.bulkAction = bulkAction;
        this.requestSupplier = requestSupplier;
        this.mapping = mapping;
        this.bulkSize = bulkSize;
    }

    /** Creates a ElasticsearchBulkIndex with some defaults. */
    public static <R> ElasticsearchBulkIndex<R> create(
        final BulkAction bulkAction,
        final Supplier<BulkRequest> requestSupplier,
        final IndexableMapping<R> mapping
    ) {
        return new ElasticsearchBulkIndex<>(
            bulkAction,
            requestSupplier,
            mapping,
            DEFAULT_BULT_SIZE
        );
    }

    @Override
    public Publisher<T> apply(final Flux<T> flux) {
        return flux.buffer(bulkSize).flatMap(items -> {
            Publisher<T> result;
            try {
                final BulkRequest req = requestSupplier.get();
                for (T item : items) {
                    final IndexableItem ii = mapping.toIndexable(item);
                    req.add(new IndexRequest(ii.getIndexName(), ii.getTypeName(), ii.getId())
                        .source(ii.getContent()));
                }
                // Why not bulkAsync? It may overwhelm the network and mess up items order
                // which may not be desired. Parallelism may be setup on the input flux.
                final BulkResponse resp = bulkAction.bulk(req);
                if (resp.hasFailures()) {
                    result = Mono.error(new IOException(
                        "There were failures in bulk request: " + resp.buildFailureMessage()
                    ));
                } else {
                    result = Flux.fromIterable(items);
                }
            } catch (Throwable e) {
                result = Mono.error(e);
            }
            return result;
        });
    }
}
