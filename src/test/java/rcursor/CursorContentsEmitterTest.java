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

    private static final String SQL = "select 1";
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(1);
    private static final Times ONCE = new Times(1);
    private static final Times TWICE = new Times(2);

    private Connection con;
    private ConnectionSupplier conSupplier;
    private ConnectionDisposer conDisposer;
    private PSCreator psCreator;
    private RSMapping<Entity> rsMapping;
    private PreparedStatement ps;
    private ResultSet rs;

    @AfterAll
    static void afterAll() throws Exception {
        EXECUTOR.shutdown();
        EXECUTOR.awaitTermination(1, TimeUnit.SECONDS);
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void beforeEach() throws Exception {
        con = mock(Connection.class);
        conSupplier = mock(ConnectionSupplier.class);
        conDisposer = mock(ConnectionDisposer.class);
        psCreator = mock(PSCreator.class);
        rsMapping = mock(RSMapping.class);
        ps = mock(PreparedStatement.class);
        rs = mock(ResultSet.class);
        when(conSupplier.get()).thenReturn(con);
        doAnswer(inv -> { ((Connection) inv.getArgument(0)).close(); return null; })
            .when(conDisposer).dispose(con);
        doAnswer(inv -> ((Connection) inv.getArgument(0)).prepareStatement(SQL))
            .when(psCreator).create(con);
        doAnswer(inv -> mapEntity((inv.getArgument(0)))).when(rsMapping).mapNext(rs);
        when(con.prepareStatement(SQL)).thenReturn(ps);
        when(ps.execute()).thenReturn(true);
        when(ps.getResultSet()).thenReturn(rs);
    }

    /** Side effect. Makes {@link #rs} to return 1 row. */
    private void setupOneEntityRS() throws Exception {
        when(rs.next())
            .thenAnswer(inv -> {
                Thread.sleep(100);
                return true;
            })
            .thenReturn(false);
        when(rs.getString(1))
            .thenReturn("id1")
            .thenThrow(new SQLException("No more results"));
    }

    /** Side effect. Makes {@link #rs} to return 2 rows. */
    private void setupTwoEntitiesRS() throws Exception {
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

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(ps, ONCE).execute();
        verify(ps, ONCE).getResultSet();
        verify(ps, ONCE).close();

        verify(rs, TWICE).next();
        verify(rs, ONCE).getString(1);
        verify(rs, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con);
        verify(rsMapping, ONCE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void successAsync() throws Exception {
        setupOneEntityRS();
        final CursorContentsEmitter<Entity> emitter = new CursorContentsEmitter<>(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            EXECUTOR,
            10,
            100L
        );

        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNext(new Entity("id1"))
            .verifyComplete();

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(ps, ONCE).execute();
        verify(ps, ONCE).getResultSet();
        verify(ps, ONCE).close();

        verify(rs, TWICE).next();
        verify(rs, ONCE).getString(1);
        verify(rs, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con);
        verify(rsMapping, ONCE).mapNext(rs);

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
            EXECUTOR
        );

        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNext(new Entity("id1"))
            .verifyErrorMessage("Oops");

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(ps, ONCE).execute();
        verify(ps, ONCE).getResultSet();
        verify(ps, ONCE).close();

        verify(rs, TWICE).next();
        verify(rs, TWICE).getString(1);
        verify(rs, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con);
        verify(rsMapping, TWICE).mapNext(rs);

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
            EXECUTOR
        );
        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .verifyErrorMessage("Oops");

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(ps, ONCE).execute();
        verify(ps, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void prepareStatementFailure() throws Exception {
        reset(con);
        when(con.prepareStatement(SQL)).thenThrow(new SQLException("Oops"));
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            EXECUTOR
        );
        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .verifyErrorMessage("Oops");

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void sinkFailure() throws Exception {
        setupTwoEntitiesRS();
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            EXECUTOR
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

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(ps, ONCE).execute();
        verify(ps, ONCE).getResultSet();
        verify(ps, ONCE).close();

        verify(rs, new Times(3)).next();
        verify(rs, TWICE).getString(1);
        verify(rs, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con);
        verify(rsMapping, TWICE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conSupplier, conDisposer, rsMapping);
    }

    @Test
    void sinkCancel() throws Exception {
        setupTwoEntitiesRS();

        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            rsMapping,
            EXECUTOR
        );

        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNext(new Entity("id1"))
            .thenAwait(Duration.ofMillis(200))
            .thenCancel()
            .verify();

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(ps, ONCE).execute();
        verify(ps, ONCE).getResultSet();
        verify(ps, ONCE).close();

        verify(rs, new Times(3)).next();
        verify(rs, TWICE).getString(1);
        verify(rs, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con);
        verify(rsMapping, TWICE).mapNext(rs);

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