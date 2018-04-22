package rcursor.function;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * PSCreator.
 */
@FunctionalInterface
public interface PSCreator {
    PreparedStatement create(Connection con) throws SQLException;
}
