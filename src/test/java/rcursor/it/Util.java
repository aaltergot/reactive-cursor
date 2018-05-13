package rcursor.it;

import java.util.Optional;

/**
 * Util.
 */
public final class Util {

    private Util() {
    }

    static int getPostgresPort() {
        return sysProp("rcursor.it.pg.port").map(Integer::parseInt).orElse(5432);
    }

    static int getElasticsearchPort() {
        return sysProp("rcursor.it.es.port").map(Integer::parseInt).orElse(9200);
    }

    private static Optional<String> sysProp(final String name) {
        return Optional.ofNullable(System.getProperty(name)).filter(s -> !s.isEmpty());
    }
}
