package rcursor.function;

import java.sql.Connection;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.junit.jupiter.api.Assertions.*;

/**
 * ConnectionManagerTest.
 */
class ConnectionManagerTest {

    @Test
    void withConnection() throws Exception {
        final Connection con = Mockito.mock(Connection.class);
        final ConnectionManager conMgr = ConnectionManager.withConnection(con);
        assertEquals(con, conMgr.get());
        conMgr.dispose(con);
        Mockito.verifyNoMoreInteractions(con);
    }
}