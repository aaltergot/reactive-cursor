package rcursor.elasticsearch;

import org.elasticsearch.common.xcontent.XContentBuilder;

/**
 * IndexableItem.
 */
public class IndexableItem {

    private final String indexName;
    private final String typeName;
    private final String id;
    private final XContentBuilder content;

    public IndexableItem(
        final String indexName,
        final String typeName,
        final String id,
        final XContentBuilder content
    ) {
        this.indexName = indexName;
        this.typeName = typeName;
        this.id = id;
        this.content = content;
    }

    public String getIndexName() {
        return indexName;
    }

    public String getTypeName() {
        return typeName;
    }

    public String getId() {
        return id;
    }

    public XContentBuilder getContent() {
        return content;
    }
}
