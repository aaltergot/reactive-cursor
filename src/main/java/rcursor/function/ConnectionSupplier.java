package rcursor.function;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ConnectionSupplier.
 */
@FunctionalInterface
public interface ConnectionSupplier {
    Connection get() throws SQLException;
}
