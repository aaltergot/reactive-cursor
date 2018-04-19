package rcursor.function;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ConnectionManager consists of two functions: {@link #get()} and {@link #dispose(Connection)}.
 * Such interface is enough to support all the routine related to {@link Connection} that is
 * happening inside {@link rcursor.CursorContentsEmitter}.
 */
public interface ConnectionManager extends ConnectionSupplier, ConnectionDisposer {

    /** Takes two lambdas, returns a ConnectionManager. */
    static ConnectionManager from(
        final ConnectionSupplier supplier,
        final ConnectionDisposer disposer
    ) {
        return new ConnectionManager() {
            @Override
            public void dispose(final Connection con) throws SQLException {
                disposer.dispose(con);
            }

            @Override
            public Connection get() throws SQLException {
                return supplier.get();
            }
        };
    }

    /**
     * Creates a ConnectionManager that simply returns a provided connection.
     * {@link #dispose(Connection)} will do nothing.
     * Useful if several operations should be done in single transaction.
     */
    static ConnectionManager withConnection(final Connection con) {
        return from(() -> con, c -> {});
    }

}
