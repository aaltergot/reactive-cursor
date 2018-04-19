package rcursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import rcursor.function.ConnectionManager;
import rcursor.function.PSCreator;
import rcursor.function.RSMapping;
import reactor.core.publisher.FluxSink;
import reactor.util.Logger;
import reactor.util.Loggers;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * CursorContentsEmitter produces a cold {@link reactor.core.publisher.Flux} that when
 * receives a subscription
 * 1. Acquires a {@link Connection} via provided {@link ConnectionManager}.
 * 2. Creates a {@link PreparedStatement} via provided {@link PSCreator}.
 * 3. Executes the query and looks up for the first {@link ResultSet} if present.
 * 4. Iterates over the ResultSet [a]synchronously using a provided {@link Executor}
 * 5. Feeds the {@link FluxSink} with items mapped using a provided {@link RSMapping}.
 * 6. Disposes the Connection on completion or sink cancellation.
 * <p/>
 * Typical usage:
 * <pre>
 * final Flux&lt;Entity&gt; entities = Flux.create(
 *     new CursorContentsEmitter&lt;&gt;(
 *         ConnectionManager.from(
 *             txConnection(dataSource),
 *             Connection::close
 *         ),
 *         forwardOnlyQuery("select * from my_table"),
 *         resultSet -&gt; new Entity(resultSet.getInt("id")),
 *         executor,
 *         2048,
 *         1000
 *     )
 * );
 * </pre>
 * One may find {@link #create} factory method useful.
 */
public final class CursorContentsEmitter<T> implements Consumer<FluxSink<T>> {

    private static final Logger logger = Loggers.getLogger(CursorContentsEmitter.class);

    private static final int DEFAULT_BUFFER_SIZE = 2048;
    private static final long DEFAULT_QUEUE_INTERVAL = 1000;

    private final ConnectionManager conMgr;
    private final PSCreator psCreator;
    private final RSMapping<T> rsMapping;
    private final Executor executor;
    private final int bufferSize;
    private final long queueMillis;

    /**
     * Constructs a CursorContentsEmitter;
     * @param conMgr {@link ConnectionManager} instance.
     * @param psCreator {@link PSCreator} function.
     * @param rsMapping {@link RSMapping} function.
     * @param executor {@link Executor} instance.
     * @param bufferSize Internal buffer size.
     * @param queueMillis Queue operations (offer and poll) interval in milliseconds.
     */
    public CursorContentsEmitter(
        final ConnectionManager conMgr,
        final PSCreator psCreator,
        final RSMapping<T> rsMapping,
        final Executor executor,
        final int bufferSize,
        final long queueMillis
    ) {
        this.conMgr = conMgr;
        this.psCreator = psCreator;
        this.rsMapping = rsMapping;
        this.executor = executor;
        this.bufferSize = bufferSize;
        this.queueMillis = queueMillis;
    }

    /** Creates a CursorContentsEmitter with some defaults. */
    public static <R> CursorContentsEmitter<R> create(
        final ConnectionManager conMgr,
        final PSCreator psCreator,
        final RSMapping<R> rsMapping,
        final Executor executor
    ) {
        return new CursorContentsEmitter<>(
            conMgr, psCreator, rsMapping, executor, DEFAULT_BUFFER_SIZE, DEFAULT_QUEUE_INTERVAL
        );
    }

    @Override
    public void accept(final FluxSink<T> sink) {
        final CursorState state = new CursorState();
        // onCancel callback will stop async rs reading
        // also state.isInProgress() => !sink.isCancelled()
        sink.onCancel(state::cancel);
        try {
            final Connection con = conMgr.get();
            try {
                try (PreparedStatement ps = psCreator.create(con)) {
                    final boolean hasRS = ps.execute();
                    if (hasRS) {
                        final LinkedBlockingDeque<T> queue = new LinkedBlockingDeque<>(bufferSize);
                        try (final ResultSet rs = ps.getResultSet()) {  // hasRS => not null
                            final Lock lock = new ReentrantLock();
                            lock.lock();
                            try {
                                final Thread subscriberThread = Thread.currentThread();
                                executor.execute(() -> {
                                    lock.lock();
                                    try {
                                        if (Thread.currentThread().equals(subscriberThread)) {
                                            // we are in single-threaded mode
                                            logger.debug("Reactive Cursor " +
                                                         "is running in single-threaded mode");
                                            readResultSetSync(rs, state, sink);
                                        } else {
                                            readResultSetAsync(rs, state, queue);
                                        }
                                    } catch (Throwable e) {
                                        state.exception(e);
                                    } finally {
                                        lock.unlock();
                                    }
                                });
                            } finally {
                                lock.unlock();
                            }
                            while (!sink.isCancelled()
                                   && (!queue.isEmpty() || state.isInProgress())) {
                                T next = null;
                                while (next == null
                                       && (!queue.isEmpty() || state.isInProgress())) {
                                    try {
                                        // if producer is slower than consumer
                                        // the queue may be empty
                                        next = queue.poll(queueMillis, MILLISECONDS);
                                    } catch (Throwable e) {
                                        state.exception(e);
                                    }
                                }
                                if (next != null && !sink.isCancelled()) {
                                    sink.next(next);
                                }
                            }
                        }
                    }
                }
            } finally {
                conMgr.dispose(con);
            }
        } catch (Throwable e) {
            state.exception(e);
        }
        if (!sink.isCancelled()) {
            if (state.isTerminated()) {
                sink.complete();
            } else if(state.isException()) {
                sink.error(state.getException());
            }
        }
    }

    private void readResultSetAsync(
        final ResultSet rs,
        final CursorState state,
        final BlockingDeque<T> queue
    ) {
        try {
            while (state.isInProgress() && rs.next()) {
                boolean added = false;
                while (!added && state.isInProgress()) {
                    // if consumer is slower than producer the queue may be full
                    added = queue.offer(rsMapping.mapNext(rs), queueMillis, MILLISECONDS);
                }
            }
            state.terminate();
        } catch (Throwable e) {
            state.exception(e);
        }
    }

    private void readResultSetSync(
        final ResultSet rs,
        final CursorState state,
        final FluxSink<T> sink
    ) {
        try {
            while (state.isInProgress() && rs.next()) {
                sink.next(rsMapping.mapNext(rs));
            }
            state.terminate();
        } catch (Throwable e) {
            state.exception(e);
        }
    }

    private static final class CursorState {
        private int s = 0; // 0-in progress, 1-terminated gracefully, 2-exception, 3-cancelled
        private Throwable ex = null;
        synchronized boolean isInProgress() { return s == 0; }
        synchronized boolean isTerminated() { return s == 1; }
        synchronized boolean isException() { return s == 2; }
        synchronized void terminate() { if (s == 0) s = 1; }
        synchronized void exception(Throwable e) { if (s == 0) { s = 2; ex = e; } }
        synchronized void cancel() { s = 3; }
        synchronized Throwable getException() { return ex; }
    }

}
