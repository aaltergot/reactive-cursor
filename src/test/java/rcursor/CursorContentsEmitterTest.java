package rcursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import rcursor.function.ConnectionDisposer;
import rcursor.function.ConnectionManager;
import rcursor.function.ConnectionSupplier;
import rcursor.function.PSCreator;
import rcursor.function.RSMapping;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * CursorContentsEmitterTest.
 */
class CursorContentsEmitterTest {

    private final String sql = "select 1";
    private final Connection con = mock(Connection.class);
    private final ConnectionSupplier conSupplier = mock(ConnectionSupplier.class);
    private final ConnectionDisposer conDisposer = mock(ConnectionDisposer.class);
    private final PSCreator psCreator = mock(PSCreator.class);
    @SuppressWarnings("unchecked")
    private final RSMapping<Entity> rsMapping = mock(RSMapping.class);
    private final PreparedStatement ps = mock(PreparedStatement.class);
    private final ResultSet rs = mock(ResultSet.class);

    private final Times once = new Times(1);
    private final Times twice = new Times(2);

    private static final ExecutorService executor = Executors.newFixedThreadPool(1);

    @AfterAll
    static void afterAll() throws Exception {
        executor.shutdown();
        executor.awaitTermination(1, TimeUnit.SECONDS);
    }

    @BeforeEach
    void beforeEach() throws Exception {
        reset(con, conSupplier, conDisposer, psCreator, rsMapping, ps, rs);
        when(conSupplier.get()).thenReturn(con);
        doAnswer(inv -> { ((Connection) inv.getArgument(0)).close(); return null; })
            .when(conDisposer).dispose(con);
        doAnswer(inv -> ((Connection) inv.getArgument(0)).prepareStatement(sql))
            .when(psCreator).create(con);
        doAnswer(inv -> mapEntity((inv.getArgument(0)))).when(rsMapping).mapNext(rs);
        when(con.prepareStatement(sql)).thenReturn(ps);
        when(ps.execute()).thenReturn(true);
        when(ps.getResultSet()).thenReturn(rs);
    }

    /** Side effect. Makes {@link #rs} to return 1 row. */
    private void setupOneEntityRS() throws Exception {
        reset(rs);
        when(rs.next())
            .thenAnswer(inv -> {
                Thread.sleep(100);
                return true;
            })
            .thenReturn(false);
        when(rs.getString(1))
            .thenReturn("id1")
            .thenThrow(new SQLException("No more results, shouldn't be here"));
    }

    /** Side effect. Makes {@link #rs} to return 2 rows. */
    private void setupTwoEntitiesRS() throws Exception {
        reset(rs);
        when(rs.next())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rs.getString(1))
            .thenReturn("id1")
            .thenReturn("id2")
            .thenThrow(new SQLException("No more results, shouldn't be here"));
    }

    @Test
    void successSync() throws Exception {
        setupOneEntityRS();
        final CursorContentsEmitter<Entity> emitter = new CursorContentsEmitter<>(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            Runnable::run,
            10,
            100L
        );

        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNext(new Entity("id1"))
            .verifyComplete();

        verify(con, once).prepareStatement(sql);
        verify(con, once).close();

        verify(ps, once).execute();
        verify(ps, once).getResultSet();
        verify(ps, once).close();

        verify(rs, twice).next();
        verify(rs, once).getString(1);
        verify(rs, once).close();

        verify(conSupplier, once).get();
        verify(conDisposer, once).dispose(con);
        verify(rsMapping, once).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void successAsync() throws Exception {
        setupOneEntityRS();
        final CursorContentsEmitter<Entity> emitter = new CursorContentsEmitter<>(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            executor,
            10,
            100L
        );

        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNext(new Entity("id1"))
            .verifyComplete();

        verify(con, once).prepareStatement(sql);
        verify(con, once).close();

        verify(ps, once).execute();
        verify(ps, once).getResultSet();
        verify(ps, once).close();

        verify(rs, twice).next();
        verify(rs, once).getString(1);
        verify(rs, once).close();

        verify(conSupplier, once).get();
        verify(conDisposer, once).dispose(con);
        verify(rsMapping, once).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void fetchFailure() throws Exception {
        when(rs.next())
            .thenReturn(true)
            .thenReturn(true)
            .thenReturn(false);
        when(rs.getString(1))
            .thenReturn("id1")
            .thenThrow(new SQLException("Oops"));

        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            executor
        );

        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNext(new Entity("id1"))
            .verifyErrorMessage("Oops");

        verify(con, once).prepareStatement(sql);
        verify(con, once).close();

        verify(ps, once).execute();
        verify(ps, once).getResultSet();
        verify(ps, once).close();

        verify(rs, twice).next();
        verify(rs, twice).getString(1);
        verify(rs, once).close();

        verify(conSupplier, once).get();
        verify(conDisposer, once).dispose(con);
        verify(rsMapping, twice).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void executeFailure() throws Exception {
        reset(ps);
        when(ps.execute()).thenThrow(new SQLException("Oops"));
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            executor
        );
        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .verifyErrorMessage("Oops");

        verify(con, once).prepareStatement(sql);
        verify(con, once).close();

        verify(ps, once).execute();
        verify(ps, once).close();

        verify(conSupplier, once).get();
        verify(conDisposer, once).dispose(con);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void prepareStatementFailure() throws Exception {
        reset(con);
        when(con.prepareStatement(sql)).thenThrow(new SQLException("Oops"));
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            executor
        );
        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .verifyErrorMessage("Oops");

        verify(con, once).prepareStatement(sql);
        verify(con, once).close();

        verify(conSupplier, once).get();
        verify(conDisposer, once).dispose(con);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void sinkFailure() throws Exception {
        setupTwoEntitiesRS();
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            executor
        );

        final AtomicReference<Entity> first = new AtomicReference<>();
        final AtomicReference<Throwable> error = new AtomicReference<>();
        final AtomicBoolean completed = new AtomicBoolean(false);

        Flux.create(emitter)
            .subscribe(
                e -> {
                    if (!first.compareAndSet(null, e)) {
                        throw new RuntimeException("Oops");
                    }
                },
                error::set,
                () -> completed.set(true)
            );

        assertEquals(first.get(), new Entity("id1"));
        assertEquals(error.get().getMessage(), "Oops");
        assertFalse(completed.get());

        verify(con, once).prepareStatement(sql);
        verify(con, once).close();

        verify(ps, once).execute();
        verify(ps, once).getResultSet();
        verify(ps, once).close();

        verify(rs, new Times(3)).next();
        verify(rs, twice).getString(1);
        verify(rs, once).close();

        verify(conSupplier, once).get();
        verify(conDisposer, once).dispose(con);
        verify(rsMapping, twice).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void sinkCancel() throws Exception {
        setupTwoEntitiesRS();

        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            executor
        );

        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNext(new Entity("id1"))
            .thenAwait(Duration.ofMillis(200))
            .thenCancel()
            .verify();

        verify(con, once).prepareStatement(sql);
        verify(con, once).close();

        verify(ps, once).execute();
        verify(ps, once).getResultSet();
        verify(ps, once).close();

        verify(rs, new Times(3)).next();
        verify(rs, twice).getString(1);
        verify(rs, once).close();

        verify(conSupplier, once).get();
        verify(conDisposer, once).dispose(con);
        verify(rsMapping, twice).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    private static Entity mapEntity(final ResultSet rs) throws SQLException {
        return new Entity(rs.getString(1));
    }

    private static class Entity {

        String id;

        Entity(final String id) {
            this.id = id;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            final Entity entity = (Entity) o;
            return Objects.equals(id, entity.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }
    }

}