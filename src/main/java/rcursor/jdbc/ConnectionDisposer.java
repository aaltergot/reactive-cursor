package rcursor.jdbc;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ConnectionDisposer.
 */
@FunctionalInterface
public interface ConnectionDisposer {
    void dispose(Connection con, boolean wasError) throws SQLException;
}
