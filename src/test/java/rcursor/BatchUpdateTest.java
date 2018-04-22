package rcursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.internal.verification.Times;
import rcursor.function.ConnectionDisposer;
import rcursor.function.ConnectionManager;
import rcursor.function.ConnectionSupplier;
import rcursor.function.PSCreator;
import rcursor.function.PSMapping;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * BatchUpdateTest.
 */
class BatchUpdateTest {

    private static final String SQL = "insert into my_table (id) values (?)";
    private static final Times ONCE = new Times(1);
    private static final Times TWICE = new Times(2);

    private Connection con;
    private ConnectionSupplier conSupplier;
    private ConnectionDisposer conDisposer;
    private PSCreator psCreator;
    private PSMapping<Entity> psMapping;
    private PreparedStatement ps;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void beforeEach() throws Exception {
        con = mock(Connection.class);
        conSupplier = mock(ConnectionSupplier.class);
        conDisposer = mock(ConnectionDisposer.class);
        psCreator = mock(PSCreator.class);
        psMapping = mock(PSMapping.class);
        ps = mock(PreparedStatement.class);
        when(conSupplier.get()).thenReturn(con);
        doAnswer(inv -> {
            final Connection con = inv.getArgument(0);
            if (inv.getArgument(1)) {
                con.rollback();
            } else {
                con.commit();
            }
            con.close();
            return null;
        }).when(conDisposer).dispose(any(), anyBoolean());
        doAnswer(inv -> ((Connection) inv.getArgument(0)).prepareStatement(SQL))
            .when(psCreator).create(any());
        doAnswer(inv -> {
            Entity.mapPrepared(inv.getArgument(0), inv.getArgument(1));
            return null;
        }).when(psMapping).mapPrepared(any(), any());
        when(con.prepareStatement(SQL)).thenReturn(ps);
        when(ps.executeBatch()).thenReturn(new int[] {1});
    }

    private static Flux<Entity> mkEntities(final int count) {
        return Flux
            .<Entity, Integer>generate(
                () -> 1,
                (i, sink) -> {
                    sink.next(new Entity("id" + i));
                    return i + 1;
                }
            )
            .take(count);
    }

    @Test
    void success() throws Exception {
        final BatchUpdate<Entity> batchUpdate = new BatchUpdate<>(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            psMapping,
            2,
            false
        );

        StepVerifier.create(mkEntities(10).compose(batchUpdate))
            .expectSubscription()
            .expectNextCount(10)
            .verifyComplete();

        final BatchUpdate.State state = batchUpdate.currentState();
        assertTrue(state.isCompleted());
        assertEquals(5, state.batchesProcessed());
        assertEquals(10, state.itemsProcessed());
        assertEquals(5, state.rowsUpdated());
        assertTrue(state.toString().contains("COMPLETED"));

        StepVerifier.create(batchUpdate.completion())
            .expectSubscription()
            .expectNextMatches(state1 ->
                state.equals(state1) && state.hashCode() == state1.hashCode()
            )
            .verifyComplete();

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).commit();
        verify(con, ONCE).close();

        verify(ps, new Times(10)).addBatch();
        verify(ps, new Times(10)).setString(eq(1), anyString());
        verify(ps, new Times(5)).executeBatch();
        verify(ps, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con, false);
        verify(psCreator, ONCE).create(con);
        verify(psMapping, new Times(10)).mapPrepared(eq(ps), any());

        verifyNoMoreInteractions(ps, con, conSupplier, conDisposer, psCreator, psMapping);
    }

    @Test
    void execUpdateFailure() throws Exception {
        reset(ps);
        when(ps.executeBatch()).thenThrow(new SQLException("Oops"));
        final BatchUpdate<Entity> batchUpdate = BatchUpdate.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            psMapping
        );

        StepVerifier.create(mkEntities(2).compose(batchUpdate))
            .expectSubscription()
            .verifyError();

        final BatchUpdate.State state = batchUpdate.currentState();
        assertTrue(state.isError());
        assertEquals(0, state.batchesProcessed());
        assertEquals(0, state.itemsProcessed());
        assertEquals(0, state.rowsUpdated());

        StepVerifier.create(batchUpdate.completion())
            .expectSubscription()
            .expectError();

        verify(con, ONCE).prepareStatement(SQL);
        verify(con, ONCE).rollback();
        verify(con, ONCE).close();

        verify(ps, TWICE).addBatch();
        verify(ps, TWICE).setString(eq(1), anyString());
        verify(ps, ONCE).executeBatch();
        verify(ps, ONCE).close();

        verify(conSupplier, ONCE).get();
        verify(conDisposer, ONCE).dispose(con, true);
        verify(psCreator, ONCE).create(con);
        verify(psMapping, TWICE).mapPrepared(eq(ps), any());

        verifyNoMoreInteractions(ps, con, conSupplier, conDisposer, psCreator, psMapping);
    }

    @Test
    void alwaysAcquireConnection() throws Exception {
        final DataSource dataSource = mock(DataSource.class);
        final List<Connection> cons = new ArrayList<>(3);
        when(dataSource.getConnection()).then(inv -> {
            final Connection con = mock(Connection.class);
            when(con.prepareStatement(SQL)).thenReturn(ps);
            cons.add(con);
            return con;
        });

        final BatchUpdate<Entity> batchUpdate = new BatchUpdate<>(
            ConnectionManager.withTransaction(dataSource),
            psCreator,
            psMapping,
            2,
            true
        );

        StepVerifier.create(mkEntities(5).compose(batchUpdate))
            .expectSubscription()
            .expectNextCount(5)
            .verifyComplete();

        final BatchUpdate.State state = batchUpdate.currentState();
        assertTrue(state.isCompleted());
        assertEquals(3, state.batchesProcessed());
        assertEquals(5, state.itemsProcessed());
        assertEquals(3, state.rowsUpdated());

        StepVerifier.create(batchUpdate.completion())
            .expectSubscription()
            .expectNext(state)
            .verifyComplete();

        for (Connection con : cons) {
            verify(con, ONCE).prepareStatement(SQL);
            verify(con, ONCE).commit();
            verify(con, ONCE).close();
        }

        verify(ps, new Times(5)).addBatch();
        verify(ps, new Times(5)).setString(eq(1), anyString());
        verify(ps, new Times(3)).executeBatch();
        verify(ps, new Times(3)).close();

        verify(psCreator, new Times(3)).create(any());
        verify(psMapping, new Times(5)).mapPrepared(eq(ps), any());

        verify(dataSource, new Times(3)).getConnection();

        verifyNoMoreInteractions(ps, dataSource, conSupplier, conDisposer, psCreator, psMapping);
    }

    @Test
    void concurrentUsage() {
        final BatchUpdate<Entity> batchUpdate = BatchUpdate.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            psMapping
        );

        StepVerifier.create(mkEntities(1).compose(batchUpdate))
            .expectSubscription()
            .expectNextCount(1)
            .verifyComplete();

        StepVerifier.create(mkEntities(1).compose(batchUpdate))
            .expectSubscription()
            .verifyError();
    }

    @Test
    void closePSFailure() throws Exception {
        doThrow(new SQLException("Oops")).when(ps).close();
        final BatchUpdate<Entity> batchUpdate = BatchUpdate.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            psMapping
        );

        StepVerifier.create(mkEntities(1).compose(batchUpdate))
            .expectSubscription()
            .expectNextCount(1)
            .verifyError();
//            .verifyErrorMessage("Oops");
    }

    @Test
    void closeConFailure() throws Exception {
        doThrow(new SQLException("Oops")).when(con).close();
        final BatchUpdate<Entity> batchUpdate = BatchUpdate.create(
            ConnectionManager.from(conSupplier, conDisposer),
            psCreator,
            psMapping
        );

        StepVerifier.create(mkEntities(1).compose(batchUpdate))
            .expectSubscription()
            .expectNextCount(1)
            .verifyError();
    }
}