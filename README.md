# reactive-postgres

## Installation

## Usage

## Design

## Performance

## Limitations

* Queries require an unique column which serves to identify rows and
  changes to them. Which column this is is configured through
  `uniqueColumn` query option. By default is `id`.
* Order of rows in query results are ignored when determining changes.
  Order still matters when selecting which rows are in query results
  through `ORDER BY X LIMIT Y` pattern.
* Currently it requires a patched version of PostgreSQL. A [Docker image with
  patches applied is available](https://github.com/mitar/docker-postgres). Hopefully
  in the future these patches will be merged in.
  * [Temporary materialized views](https://commitfest.postgresql.org/21/1951/)
  * [Triggers on materialized views](https://commitfest.postgresql.org/21/1953/)

## API

### `Manager`

Manager manages a pool of connections to PostgreSQL and organizes reactive queries
over them. You should initialize one instance of it and use it for your reactive queries

##### `constructor([options])`

##### `async start()`

##### `async stop([error])`

##### `async query(query[, options={}])`

##### `'start'` event `()`

##### `'connect'` event `(client)`

##### `'disconnect'` event `(client)`

##### `'error'` event `(error[, client])`

##### `'stop'` event `([error])`

### `ReactiveQueryHandle`

Reactive query handles can be used as an [`EventEmitter`](https://nodejs.org/api/events.html#events_class_eventemitter)
or a [`Readable` stream](https://nodejs.org/api/stream.html#stream_readable_streams).

Constructor is seen as private and you should not create instances of `ReactiveQueryHandle`
yourself but always through `Manager`'s `query` method. The method also passes all options
to `ReactiveQueryHandle`.

#### `EventEmitter` API

There are more methods, properties, and options available through the
[`EventEmitter` base class](https://nodejs.org/api/events.html#events_class_eventemitter).

##### `async start()`

##### `async stop([error])`

##### `async flush()`

##### `async refresh()`

##### `on(eventName, listener)` and `addListener(eventName, listener)`

Adds the `listener` function to the end of the listeners array for the event named `eventName`.
[More information](https://nodejs.org/api/events.html#events_emitter_on_eventname_listener).

##### `once(eventName, listener)`

Adds a *one-time* `listener` function for the event named `eventName`.
[More information](https://nodejs.org/api/events.html#events_emitter_once_eventname_listener).

##### `off(eventName, listener)` and `removeListener(eventName, listener)`

Removes the specified `listener` from the listener array for the event named `eventName`.
[More information](https://nodejs.org/api/events.html#events_emitter_removelistener_eventname_listener).

##### `'start'` event `()`

##### `'ready'` event `()`

##### `'refresh'` event `()`

##### `'insert'` event `(row)`

##### `'update'` event `(row, columns)`

##### `'delete'` event `(row)`

##### `'error'` event `(error)`

##### `'stop'` event `([error])`

#### `Readable` stream API

There are more methods, properties, and options available through the
[`Readable` stream base class](https://nodejs.org/api/stream.html#stream_readable_streams).

##### `read([size])`

##### `pipe(destination[, options])`

##### `destroy([error])`

##### 'data' event `(chunk)`

##### 'readable' event `()`

##### 'error' event `(error)`

##### 'close' event `()`

## Related projects

* [pg-live-select](https://github.com/numtel/pg-live-select) – when the query's sources change, the client reruns the
  query to obtain updated rows, which are identified through hashes of their complete content and client then computes a diff,
  on the other hand, this package maintains cached results of the query in a temporary materialized view and reuses
  computation of a diff inside `REFRESH MATERIALIZED VIEW CONCURRENTLY` PostgreSQL command, only information which rows
  and columns changed are first provided to the client and client can then query only those changed columns
* [pg-live-query](https://github.com/nothingisdead/pg-live-query) – adds revision columns to sources and additional
  temporary table which stores information how those revisions map to queries, all this then allows computing a diff
  inside the database, but the approach does not allow 
