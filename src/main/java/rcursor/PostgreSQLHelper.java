package rcursor;

import java.sql.PreparedStatement;
import rcursor.function.PSCreator;

import static java.sql.ResultSet.CONCUR_READ_ONLY;
import static java.sql.ResultSet.TYPE_FORWARD_ONLY;

/**
 * Useful functions to work with PostgreSQL cursors.
 * See https://jdbc.postgresql.org/documentation/94/query.html#fetchsize-example.
 */
public final class PostgreSQLHelper {

    public static final int DEFAULT_FETCH_SIZE = 1024;

    private PostgreSQLHelper() {
    }

    public static PSCreator forwardOnlyQuery(final String sql, final int fetchSize) {
        return con -> {
            final PreparedStatement ps =
                con.prepareStatement(sql, TYPE_FORWARD_ONLY, CONCUR_READ_ONLY);
            ps.setFetchSize(fetchSize);
            return ps;
        };
    }

    public static PSCreator forwardOnlyQuery(final String sql) {
        return forwardOnlyQuery(sql, DEFAULT_FETCH_SIZE);
    }
}
