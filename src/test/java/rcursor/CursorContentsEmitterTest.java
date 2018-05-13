package rcursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import org.mockito.stubbing.OngoingStubbing;
import rcursor.jdbc.ConnectionManager;
import rcursor.jdbc.PSCreator;
import rcursor.jdbc.RSMapping;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * CursorContentsEmitterTest.
 */
class CursorContentsEmitterTest {

    private static final String SQL = "select id from my_table";
    private static final ExecutorService EXECUTOR = Executors.newFixedThreadPool(1);
    private static final Times ONCE = new Times(1);
    private static final Times TWICE = new Times(2);

    private Connection con;
    private ConnectionManager conMgr;
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
    @SuppressWarnings({"Convert2Lambda", "Anonymous2MethodRef"})
    void beforeEach() throws Exception {
        con = mock(Connection.class);
        // cannot spy lambdas
        conMgr = spy(
            new ConnectionManager() {
                @Override
                public void dispose(
                    final Connection con,
                    final boolean wasError
                ) throws SQLException {
                    con.close();
                }
                @Override
                public Connection get() {
                    return con;
                }
            }
        );
        psCreator = spy(
            new PSCreator() {
                @Override
                public PreparedStatement create(final Connection con) throws SQLException {
                    return con.prepareStatement(SQL);
                }
            }
        );
        rsMapping = spy(
            new RSMapping<Entity>() {
                @Override
                public Entity mapNext(final ResultSet rs) throws SQLException {
                    return Entity.mapEntity(rs);
                }
            }
        );
        ps = mock(PreparedStatement.class);
        rs = mock(ResultSet.class);
        when(con.prepareStatement(SQL)).thenReturn(ps);
        when(ps.execute()).thenReturn(true);
        when(ps.getResultSet()).thenReturn(rs);
    }

    /** Side effect. Makes {@link #rs} to return specified number of rows. */
    private void setupResultSetRows(final int count) throws Exception {
        reset(rs);
        OngoingStubbing<Boolean> next = when(rs.next());
        for (int i = 1; i <= count; i++) {
            next = next.thenReturn(true);
        }
        next.thenReturn(false);
        OngoingStubbing<String> getString = when(rs.getString(1));
        for (int i = 1; i <= count; i++) {
            getString = getString.thenReturn("id" + i);
        }
    }

    @Test
    void successSync() throws Exception {
        setupResultSetRows(1);
        final CursorContentsEmitter<Entity> emitter = new CursorContentsEmitter<>(
            conMgr,
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

        verify(conMgr, ONCE).get();
        verify(conMgr, ONCE).dispose(con, false);
        verify(rsMapping, ONCE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
    }

    @Test
    void successAsync() throws Exception {
        setupResultSetRows(1);
        final CursorContentsEmitter<Entity> emitter = new CursorContentsEmitter<>(
            conMgr,
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

        verify(conMgr, ONCE).get();
        verify(conMgr, ONCE).dispose(con, false);
        verify(rsMapping, ONCE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
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
            conMgr,
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

        verify(conMgr, ONCE).get();
        verify(conMgr, ONCE).dispose(con, true);
        verify(rsMapping, TWICE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
    }

    @Test
    void executeFailure() throws Exception {
        reset(ps);
        when(ps.execute()).thenThrow(new SQLException("Oops"));
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            conMgr,
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

        verify(conMgr, ONCE).get();
        verify(conMgr, ONCE).dispose(con, true);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
    }

    @Test
    void prepareStatementFailure() throws Exception {
        reset(con);
        when(con.prepareStatement(SQL)).thenThrow(new SQLException("Oops"));
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            conMgr,
            psCreator,
            rsMapping,
            EXECUTOR
        );
        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .verifyErrorMessage("Oops");

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).close();

        verify(conMgr, ONCE).get();
        verify(conMgr, ONCE).dispose(con, true);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
    }

    @Test
    void sinkFailure() throws Exception {
        setupResultSetRows(2);
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            conMgr,
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

        verify(conMgr, ONCE).get();
        verify(conMgr, ONCE).dispose(con, false);
        verify(rsMapping, TWICE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
    }

    @Test
    void sinkCancel() throws Exception {
        setupResultSetRows(2);
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            conMgr,
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

        verify(conMgr, ONCE).get();
        verify(conMgr, ONCE).dispose(con, false);
        verify(rsMapping, TWICE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
    }

    @Test
    void commitFailure() throws Exception {
        setupResultSetRows(1);
        doThrow(new SQLException("Oops")).when(con).commit();
        final DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(con);
        final CursorContentsEmitter<Entity> emitter = CursorContentsEmitter.create(
            ConnectionManager.withTransaction(dataSource),
            psCreator,
            rsMapping,
            EXECUTOR
        );
        StepVerifier.create(Flux.create(emitter))
            .expectSubscription()
            .expectNextCount(1)
            .verifyErrorMessage("Oops");

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).setAutoCommit(false);
        verify(con, ONCE).commit();
        verify(con, ONCE).close();

        verify(ps, ONCE).execute();
        verify(ps, ONCE).getResultSet();
        verify(ps, ONCE).close();

        verify(rs, TWICE).next();
        verify(rs, ONCE).getString(1);
        verify(rs, ONCE).close();

        verify(rsMapping, ONCE).mapNext(rs);

        verifyNoMoreInteractions(rs, ps, con, conMgr, rsMapping);
    }
}