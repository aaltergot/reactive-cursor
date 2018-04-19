package rcursor.function;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ConnectionSupplier.
 */
public interface ConnectionSupplier {
    Connection get() throws SQLException;
}
