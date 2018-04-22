package rcursor;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * PostgreSQLHelperTest.
 */
class PostgreSQLHelperTest {

    @Test
    void forwardOnlyQuery() throws Exception {
        final Connection con = mock(Connection.class);
        final PreparedStatement ps = mock(PreparedStatement.class);
        //noinspection MagicConstant
        when(con.prepareStatement(anyString(), eq(ResultSet.TYPE_FORWARD_ONLY), anyInt()))
            .thenReturn(ps);
        assertNotNull(PostgreSQLHelper.forwardOnlyQuery("select 1").create(con));
    }
}