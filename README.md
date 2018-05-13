# Reactive Cursor
This is a set of utilities to work with JDBC ResultSet in reactive fashion.

The initial idea is to leverage cursors that are created and maintained on the database side. Most RDBMS support this feature (PostgreSQL!). This mechanism allows to fetch limited portions of data and never run out of memory. Thus potentially unlimited result sets may be processed.

Another goal is to avoid I/O blocking serialization. All JDBC actions block the worker thread while the data is being fetched and transmitted over the network. The consuming counterpart may also perform I/O. 

For instance, we query something in the database, transform rows into JSONs, then send them somewhere over HTTP. In single-threaded environment this would mean that the worker thread blocks on read operation waiting for a meaningful
chunk of data to come, applies some transformations, then blocks again until write operation is completed. Starts over with the blocking read and so on. With multiple threads there is no need to wait each other.

But multi-threading requires good abstractions to juggle the data safely and easily. This library is an attempt to provide such abstractions standing on [Project Reactor](https://projectreactor.io) foundation, helping to deal efficiently with massive ETL routines.

## Usage
### Read RDBMS Cursor
Having `my_table` populated with some `Entities` we are looking for `Flux<Entity>`. Other cool stuff a developer implements using Flux API.
```
1   Flux<Entity> entities = Flux.create(
2      CursorContentsEmitter.create(
3          ConnectionManager.withTransaction(dataSource),
4          forwardOnlyQuery("select * from my_table"),
5          resultSet -> new Entity(resultSet.getInt("id")),
6          executor
7   ));

```
1: Creating a Flux using Flux.create() method. It accepts `Consumer<FluxSink<T>>` which is satisfied by the `CursorContentsEmitter`.  
2: `CursorContentsEmitter` has a convenient factory method that drops in some default values. One may use a constructor instead. See the javadoc.  
3: `ConnectionManager` is really just two functions combined together. See the javadoc. `ConnectionManager.withTransaction` returns a closure on the `dataSource` that would produce new no-autocommit connection every time, commit/rollback when disposed.  
4: `forwardOnlyQuery` makes a function that produces `PreparedStatement` with necessary configuration. For PostgreSQL to create a cursor on database side it is required that connection is no-autocommit and the statement is created with `TYPE_FORWARD_ONLY` flag. Refer to `PostgreSQLHelper` utility class. Other databases may require something else to be configured for that purpose.  
5: Just a lambda that transforms current row into `Entity`. One may find convenient to pass method reference here.  
6: Result set fetching procedure will be submitted to provided `executor`. A good idea is to have a thread pool instance for this purpose.

See also `CursorContentsEmitterTest` for usage examples.

### Batch Insert or Update
Following construction will send all entities to the database in batches of default size.
```
    DataSource dataSource = ...
    Flux<Entity> entities = ...
    BatchUpdate<Entity> batchUpdate = BatchUpdate.create(
        ConnectionManager.withTransaction(dataSource),
        con -> con.prepareStatement("insert into my_table (id) values (?)"),
        (ps, entity) -> ps.setString(1, entity.id)
    );
    entities.compose(batchUpdate).subscribe();
```
One may want to get and inspect batch update results:
```
    // value will appear onces completed (successfully or not)
    Mono<BatchUpdate.State> stateMono = batchUpdate.completion();
    // inspect progress on the fly
    BatchUpdate.State state = batchUpdate.currentState();
```

### Elasticsearch bulk index
```
    Flux<Entity> entities = ...
    RestHighLevelClient esClient = ...
    final ElasticsearchBulkIndex<Entity> esBulk =
        ElasticsearchBulkIndex.create(
            esClient::bulk,
            BulkRequest::new,
            entity -> new IndexableItem(...)
        );
    entities.compose(esBulk).subscribe();
```

### Integration tests
Integration tests (IT) are enabled via `enable-it` Maven profile.  
it uses [docker-maven-plugin](https://github.com/fabric8io/docker-maven-plugin) under the hood to run `postgres` and `elasticsearch`.  
Tests rely on system properties set by [maven-failsafe-plugin](http://maven.apache.org/surefire/maven-failsafe-plugin/examples/system-properties.html), e.g. `rcursor.it.pg.port`, `rcursor.it.es.port` to use randomly assigned ports (this technique helps to avoid port binding failure).
To run the whole tests suite (unit+integration):
```
mvn verify -P enable-it
```
To play with IT having dependencies (postgres, ES) already running in background just configure correct sys props. Example:
```
mvn failsafe:integration-test failsafe:verify -P enable-it -Drcursor.it.pg.port=5432 -Drcursor.it.es.port=9200
```
Also defaults are provided by _rcursor.it.Util_ thus `-Drcursor.it.pg.port=5432 -Drcursor.it.es.port=9200` may be omitted.  
IT source code may give good examples how to use this library. Highly [recommended for inspection](src/test/java/rcursor/it).