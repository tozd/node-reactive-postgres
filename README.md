# reactive-postgres

## Installation

Currently it requires a patched version of PostgreSQL. A [Docker image with
patches applied is available](https://github.com/mitar/docker-postgres). Hopefully
in the future these patches will be merged in.
  * [Temporary materialized views](https://commitfest.postgresql.org/21/1951/)
  * [Triggers on materialized views](https://commitfest.postgresql.org/21/1953/)

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
* Queries cannot contain placeholders or be prepared.

## API

### `Manager`

Manager manages a pool of connections to the database and organizes reactive queries
over them. You should initialize one instance of it and use it for your reactive queries

##### `constructor([options])`

Available `options`:
* `maxConnections`, default `10`: the maximum number of connections to the database
  for reactive queries, the final number of connections is `maxConnections` + 1, for the
  extra manager's connection
* `connectionConfig`, default `{}`: PostgreSQL connection configuration,
  [more information](https://node-postgres.com/api/client#new-client-config-object-)
* `handleClass`: default `ReactiveQueryHandle`: a class to use for reactive query
  handles returned by `query` method

##### `async start()`

Initializes the manager, the database, and establishes its connection.
It emits `'start'` event.

##### `async stop([error])`

Stops all reactive queries and the manager itself.
It emits `'stop'` event. Accepts optional `error` argument which is passed
as payload in `'stop'` event.

##### `async query(query[, options={}])`

Constructs a new reactive query and returns a handle.

`query` can be any arbitrary [`SELECT`](https://www.postgresql.org/docs/devel/sql-select.html),
[`TABLE`](https://www.postgresql.org/docs/10/sql-select.html#SQL-TABLE), or
[`VALUES`](https://www.postgresql.org/docs/10/sql-values.html) command, adhering to
[limitations](#limitations).

Available `options`:
* `uniqueColumn`, default `id`: the name of an unique column in query results, used
to identify rows and changes to them
* `refreshThrottleWait`, default 100: this option controls that refresh can happen at most once
per every `refreshThrottleWait` milliseconds
  * this introduces a minimal delay between a source change and a refresh, you can
    control this delay based on requirements for a particular query
  * lower this value is, higher the load on the database will be, higher it is, lower the load
    will be
* `mode`, default `id`: in which mode to operate, it can be:
  * `id`: for every query results change, provide only which row and columns changed,
    this does not require an additional query to the database to fetch updated results data
  * `changed`: for every query results change, fetch and provide also updated results data,
    but only those columns which have changed
  * `full`: for every query results change, fetch and provide full rows of updated results data,
    both columns which have changed and those which have not
* `batchSize`, default `0`: when mode is `changed` or `full`, in how large batches
  (for how many changes) do we fetch data for one refresh, 0 means only one batch
  per refresh
  * batching introduces additional delay between a change happening and emitting of the
    corresponding change event
  * lower the number is (except for 0), more queries to the database are made to fetch data
* `types`, default `null`: [custom type parsers](https://node-postgres.com/features/queries#types)

##### `'start'` event `()`

##### `'connect'` event `(client)`

##### `'disconnect'` event `(client)`

##### `'error'` event `(error[, client])`

##### `'stop'` event `([error])`

### `ReactiveQueryHandle`

Reactive query handles can be used as an [`EventEmitter`](https://nodejs.org/api/events.html#events_class_eventemitter)
or a [`Readable` stream](https://nodejs.org/api/stream.html#stream_readable_streams),
but not both.

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
  query to obtain updated rows, which are identified through hashes of their complete content and client then computes changes,
  on the other hand, this package maintains cached results of the query in a temporary materialized view and reuses
  computation of a diff inside `REFRESH MATERIALIZED VIEW CONCURRENTLY` PostgreSQL command, only information which rows
  and columns changed are first provided to the client and client can then query only those changed columns
* [pg-live-query](https://github.com/nothingisdead/pg-live-query) – adds revision columns to sources and additional
  temporary table which stores information how those revisions map to queries, all this then allows computing changes
  inside the database, but the approach does not allow 
* [pg-query-observer](https://github.com/Richie765/pg-query-observer) – it seems like a bit cleaned and updated version
  of `pg-live-select`, but buggy and does not work with multiple parallel queries
