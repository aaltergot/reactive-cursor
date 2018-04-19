package rcursor.function;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ConnectionDisposer.
 */
public interface ConnectionDisposer {
    void dispose(Connection con) throws SQLException;
}
