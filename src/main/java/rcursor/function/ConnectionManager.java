package rcursor.function;

import java.sql.Connection;
import java.sql.SQLException;
import javax.sql.DataSource;

/**
 * ConnectionManager consists of two functions:
 * {@link #get()} and {@link #dispose(Connection, boolean)}.
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
            public void dispose(final Connection con, final boolean wasError) throws SQLException {
                disposer.dispose(con, wasError);
            }

            @Override
            public Connection get() throws SQLException {
                return supplier.get();
            }
        };
    }

    /**
     * Creates a ConnectionManager that simply returns a provided connection.
     * {@link #dispose(Connection, boolean)} will do nothing.
     * Useful if several operations should be done in single transaction.
     */
    static ConnectionManager withConnection(final Connection con) {
        return from(() -> con, (c, e) -> {});
    }

    static ConnectionManager withTransaction(final DataSource dataSource) {
        return from(
            () -> {
                final Connection con = dataSource.getConnection();
                con.setAutoCommit(false);
                return con;
            },
            (con, wasError) -> {
                SQLException toThrow = null;
                try {
                    if (wasError) {
                        con.rollback();
                    } else {
                        con.commit();
                    }
                } catch (SQLException e) {
                    toThrow = e;
                }
                con.close();
                if (toThrow != null) {
                    throw toThrow;
                }
            }
        );
    }

}
