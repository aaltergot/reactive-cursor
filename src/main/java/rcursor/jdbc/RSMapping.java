package rcursor.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * RSMapping.
 */
@FunctionalInterface
public interface RSMapping<T> {
    T mapNext(ResultSet rs) throws SQLException;
}
