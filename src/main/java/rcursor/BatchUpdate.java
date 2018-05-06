package rcursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;
import java.util.stream.IntStream;
import org.reactivestreams.Publisher;
import rcursor.jdbc.ConnectionManager;
import rcursor.jdbc.PSCreator;
import rcursor.jdbc.PSMapping;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * BatchUpdate.
 */
public final class BatchUpdate<T> implements Function<Flux<T>, Publisher<T>> {

    private static final int DEFAULT_BATCH_SIZE = 4096;

    private final ConnectionManager conMgr;
    private final PSCreator psCreator;
    private final PSMapping<T> psMapping;
    private final int batchSize;

    /** If true - acquire new and dispose {@link Connection} on every batch. */
    private final boolean alwaysAcquireConnection;

    private final State state = new State();
    private final CompletableFuture<State> stateFuture = new CompletableFuture<>();
    private final Mono<State> stateMono = Mono.fromFuture(stateFuture);
    private Connection con;
    private PreparedStatement ps;

    public BatchUpdate(
        final ConnectionManager conMgr,
        final PSCreator psCreator,
        final PSMapping<T> psMapping,
        final int batchSize,
        final boolean alwaysAcquireConnection
    ) {
        this.conMgr = conMgr;
        this.psCreator = psCreator;
        this.psMapping = psMapping;
        this.batchSize = batchSize;
        this.alwaysAcquireConnection = alwaysAcquireConnection;
    }

    /** Creates a BatchUpdate with some defaults. */
    public static <R> BatchUpdate<R> create(
        final ConnectionManager conMgr,
        final PSCreator psCreator,
        final PSMapping<R> psMapping
    ) {
        return new BatchUpdate<>(conMgr, psCreator, psMapping, DEFAULT_BATCH_SIZE, false);
    }

    public Mono<State> completion() {
        return stateMono;
    }

    public State currentState() {
        return state.copy();
    }

    private void close(final Optional<Throwable> downstreamError) {
        synchronized (state) {
            final List<Throwable> errors = new ArrayList<>(4);
            downstreamError.ifPresent(errors::add);
            state.close(errors.isEmpty());
            if (!alwaysAcquireConnection) {
                if (ps != null) {
                    try {
                        ps.close();
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }
                if (con != null) {
                    try {
                        conMgr.dispose(con, !errors.isEmpty());
                    } catch (Throwable e) {
                        errors.add(e);
                    }
                }
            }
            if (!errors.isEmpty()) {
                final RuntimeException multiple = Exceptions.multiple(errors);
                stateFuture.completeExceptionally(multiple);
                throw multiple;
            } else {
                stateFuture.complete(currentState());
            }
        }
    }

    @Override
    public Publisher<T> apply(final Flux<T> flux) {
        try {
            if (!state.isNewSync()) {
                throw new IllegalStateException("Concurrent usage is prohibited");
            }
            if (!alwaysAcquireConnection) {
                // init once
                con = conMgr.get();
                ps = psCreator.create(con);
            }
            state.start();
        } catch (Throwable e) {
            return Mono.error(e);
        }
        return flux.buffer(batchSize)
            .flatMap(items -> {
                Publisher<T> result;
                try {
                    if (alwaysAcquireConnection) {
                        con = conMgr.get();
                        ps = psCreator.create(con);
                    }
                    for (T item : items) {
                        psMapping.mapPrepared(ps, item);
                        ps.addBatch();
                    }
                    final int[] rus = ps.executeBatch();
                    state.update(items.size(), IntStream.of(rus).sum());
                    if (alwaysAcquireConnection) {
                        ps.close();
                        conMgr.dispose(con, false);
                    }
                    result = Flux.fromIterable(items);
                } catch (Throwable e) {
                    result = Mono.error(e);
                }
                return result;
            })
            .doOnComplete(() -> close(Optional.empty()))
            .doOnError(e -> close(Optional.of(e)));
    }

    /**
     * Represents state of a BatchUpdate.
     * Only immutable copies are published.
     */
    public static class State {

        /** 0-new, 1-in progress, 2-completed, 3-error */
        private int s = 0;
        private long itemsProcessed = 0;
        private long batchesProcessed = 0;
        private long rowsUpdated = 0;

        private State() {
        }

        private synchronized State copy() {
            final State state = new State();
            state.s = this.s;
            state.itemsProcessed = this.itemsProcessed;
            state.batchesProcessed = this.batchesProcessed;
            state.rowsUpdated = this.rowsUpdated;
            return state;
        }

        public synchronized boolean isNewSync() { return s == 0; }

        public boolean isNew() { return s == 0; }
        public boolean isInProgress() { return s == 1; }
        public boolean isCompleted() { return s == 2; }
        public boolean isError() { return s == 3; }
        public long itemsProcessed() { return itemsProcessed; }
        public long batchesProcessed() { return batchesProcessed; }
        public long rowsUpdated() { return rowsUpdated; }

        private synchronized void update(final long ip, final long ru) {
            itemsProcessed += ip;
            batchesProcessed += 1;
            rowsUpdated += ru;
        }

        private synchronized void start() {
            if (s == 0) {
                s = 1;
            }
        }

        private synchronized void close(final boolean success) {
            s = success ? 2 : 3;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final State state = (State) o;
            return s == state.s &&
                   itemsProcessed == state.itemsProcessed &&
                   batchesProcessed == state.batchesProcessed &&
                   rowsUpdated == state.rowsUpdated;
        }

        @Override
        public int hashCode() {
            return Objects.hash(s, itemsProcessed, batchesProcessed, rowsUpdated);
        }

        @Override
        public String toString() {
            return "State{" +
                   ( isNew() ? "NEW"
                       : isInProgress() ? "IN_PROGRESS"
                       : isCompleted() ? "COMPLETED"
                       : "ERROR"
                   ) +
                   ", itemsProcessed=" + itemsProcessed +
                   ", batchesProcessed=" + batchesProcessed +
                   ", rowsUpdated=" + rowsUpdated +
                   '}';
        }
    }
}
