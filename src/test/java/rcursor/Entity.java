package rcursor;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Objects;
import reactor.core.publisher.Flux;

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

    public static Flux<Entity> make(final int count) {
        return Flux
            .<Entity, Integer>generate(
                () -> 1,
                (i, sink) -> {
                    sink.next(new Entity("id" + i));
                    return i + 1;
                }
            )
            .take(count);
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
