package rcursor.jdbc;

import java.sql.Connection;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * ConnectionManagerTest.
 */
class ConnectionManagerTest {

    @Test
    void withConnection() throws Exception {
        final Connection con = mock(Connection.class);
        final ConnectionManager conMgr = ConnectionManager.withConnection(con);
        assertEquals(con, conMgr.get());
        conMgr.dispose(con, false);
        verifyNoMoreInteractions(con);
    }

    @Test
    void withTransaction() throws Exception {
        final Connection con = mock(Connection.class);
        final DataSource dataSource = mock(DataSource.class);
        when(dataSource.getConnection()).thenReturn(con);
        final ConnectionManager conMgr = ConnectionManager.withTransaction(dataSource);
        assertEquals(con, conMgr.get());
        verify(dataSource).getConnection();
        verify(con).setAutoCommit(false);
        verifyNoMoreInteractions(con, dataSource);
        conMgr.dispose(con, true);
        verify(con).rollback();
        verify(con).close();
        verifyNoMoreInteractions(con);
    }
}