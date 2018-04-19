# Reactive Cursor
This is a set of utilities to work with JDBC ResultSet in reactive fashion.

The initial idea is to leverage cursors that are created and maintained on the database side. Most RDBMS support this feature (PostgreSQL!). This mechanism allows to fetch limited portions of data and never run out of memory. Thus potentially unlimited result sets may be processed.

Another goal is to avoid I/O blocking serialization. All JDBC actions block the worker thread while the data is being fetched and transmitted over the network. The consuming counterpart may also perform I/O. 

For instance, we query something in the database, transform rows into JSONs, then send them somewhere over HTTP. In single-threaded environment this would mean that the worker thread blocks on read operation waiting for a meaningful
chunk of data to come, applies some transformations, then blocks again until write operation is completed. Starts over with the blocking read and so on. With multiple threads there is no need to wait each other.

But multi-threading requires good abstractions to juggle the data safely and easily. This library is an attempt to provide such abstractions standing on [Project Reactor](https://projectreactor.io) foundation, helping to deal effectively with massive ETL routines.

## Usage
### Read RDBMS Cursor
Having `my_table` populated with some `Entities` we are looking for `Flux<Entity>`. Other cool stuff a developer implements using Flux API.
```
1  final Flux<Entity> entities = Flux.create(
2     CursorContentsEmitter.create(
3         ConnectionManager.from(
4             txConnection(dataSource),
5             Connection::close
6         ),
7         forwardOnlyQuery("select * from my_table"),
8         resultSet -> new Entity(resultSet.getInt("id")),
9         executor
10 ));

```
1: Creating a Flux using Flux.create() method. It accepts `Consumer<FluxSink<T>>` which is satisfied by the `CursorContentsEmitter`.  
2: `CursorContentsEmitter` has a convenient factory method that drops in some default values. One may use a constructor instead. See the javadoc.  
3: `ConnectionManager` is really just two functions combined together. See the javadoc.  
4: `txConnection` returns a closure on the `dataSource` that would produce new no-autocommit connection every time.  
5: Dispose action when a connection is not needed anymore. One may want to keep the connection open for further processing and pass empty lambda here.  
7: `forwardOnlyQuery` makes a function that produces `PreparedStatement` with necessary configuration. For PostgreSQL to create a cursor on database side it is required that connection is no-autocommit and the statement is created with `TYPE_FORWARD_ONLY` flag. Refer to `PostgreSQLHelper` utility class. Other databases may require something else to be configured for that purpose.  
8: Just a lambda that transforms current row into `Entity`. One may find convenient to pass method reference here.  
9: Result set fetching procedure will be submitted to provided `executor`. A good idea is to have a thread pool instance for this purpose.
