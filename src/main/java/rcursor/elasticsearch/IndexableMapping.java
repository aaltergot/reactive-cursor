package rcursor.elasticsearch;

/**
 * IndexableMapping.
 */
@FunctionalInterface
public interface IndexableMapping<T> {
    IndexableItem toIndexable(T item) throws Exception;
}
