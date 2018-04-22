package rcursor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;

/**
 * Entity.
 */
public class Entity {
    public String id;

    public Entity(final String id) {
        this.id = id;
    }

    public static Entity mapEntity(final ResultSet rs) throws SQLException {
        return new Entity(rs.getString(1));
    }

    public static void mapPrepared(
        final PreparedStatement ps,
        final Entity entity
    ) throws SQLException {
        ps.setString(1, entity.id);
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final Entity entity = (Entity) o;
        return Objects.equals(id, entity.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
