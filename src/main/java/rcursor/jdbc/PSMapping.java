package rcursor.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * PSMapping.
 */
@FunctionalInterface
public interface PSMapping<T> {
    void mapPrepared(PreparedStatement ps, T item) throws SQLException;
}
