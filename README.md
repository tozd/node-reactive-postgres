# reactive-postgres

## Installation

Currently it requires a patched version of PostgreSQL. A [Docker image with
patches applied is available](https://github.com/mitar/docker-postgres). Hopefully
in the future these patches will be merged in.
  * [Temporary materialized views](https://commitfest.postgresql.org/21/1951/)
  * [Triggers on materialized views](https://commitfest.postgresql.org/21/1953/)

## Usage

## Design

Reactive queries are implemented in the following manner:

* For every reactive query, a `TEMPORARY MATERIALIZED VIEW` is created
  in the database.
* Triggers are added to all source tables for the query, so that
  when any of the source tables change, this package's client is
  notified using `LISTEN`/`NOTIFY` that a source has changed, which can
  potentially influence the results of the query.
* Triggers are added to the view so that any `INSERT`s/`UPDATE`s/`DELETE`s
  send notifications to the client about rows (and columns) changing.
  In this way client can know what has changed in the query since
  the last `REFRESH MATERIALIZED VIEW`.
* Client waits for source changed events, and throttles them based
  on `refreshThrottleWait` option. Once the delay expires, the client
  issues `REFRESH MATERIALIZED VIEW CONCURRENTLY`. This reruns the
  query and updates the view. This in turn notifies the client of
  changes which happened since the previous refresh.
* If operating in `id` mode, then those notifications are directly
  emitted as events or pushed into a stream.
* If operating in `changed` or `full` modes, notifications are batched
  together based on `batchSize` and query to retrieve updated query
  results data is made once batch is full. Results are used to populate
  data about other columns when emitting events.

## Performance

This package has known performance issues:
* A [memory leak](https://github.com/tozd/node-reactive-postgres/issues/1).
* A [deadlock or very long time to exit](https://github.com/tozd/node-reactive-postgres/issues/2).
* [Response latency is suprisingly high in comparison with other projects](https://github.com/tozd/node-reactive-postgres/issues/3).

For more information about performance comparisons of this package and related packages,
see [this benchmark tool](https://github.com/mitar/node-pg-reactivity-benchmark) and
[results at the end](https://github.com/mitar/node-pg-reactivity-benchmark#results).

## Limitations

* Queries require an unique column which serves to identify rows and
  changes to them. Which column this is is configured through
  `uniqueColumn` query option. By default is `id`.
* Order of rows in query results are ignored when determining changes.
  Order still matters when selecting which rows are in query results
  through `ORDER BY X LIMIT Y` pattern. If you care about order of
  query results, order rows on the client.
* Queries cannot contain placeholders or be prepared. You can use
  `client.escapeLiteral(...)` function to escape values when constructing
  a query.

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

Event emitted when manager starts successfully.

##### `'connect'` event `(client)`

Event emitted when a new [PostgreSQL client](https://node-postgres.com/api/client) is created
and connected. `client` is provided as an argument.

##### `'disconnect'` event `(client)`

Event emitted when a PostgreSQL client is disconnected. `client` is provided as an argument.

##### `'error'` event `(error[, client])`

Event emitted when there is an error at the manager level. `error` is provided as an argument.
If the error is associated with a PostgreSQL client, the `client` is provided as well.

##### `'stop'` event `([error])`

Event emitted when the manager stops. If it stopped because of an error,
the `error` is provided as an argument.

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

Initializes the reactive query and starts observing the reactive query, emitting events for
initial results data and later changes to results data.
It emits `'start'` event.

Having this method separate from constructing a reactive query allows attaching event handlers
before starting, so that no events are missed.

##### `async stop([error])`

Stops the reactive query. 
It emits `'stop'` event. Accepts optional `error` argument which is passed
as payload in `'stop'` event.

##### `async refresh()`

Forces the refresh of the reactive query, computation of changes, and emitting
relevant events. This can override `refreshThrottleWait` option which otherwise
controls the minimal delay between a source change and a refresh.

##### `async flush()`

When operating in `changed` or `full` mode, changes are batched together before
a query to fetch data is made. This method forces the query to be made using
currently known changes instead of waiting for a batch as configured by the
`batchSize` option.

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

Event emitted when reactive query starts successfully.

##### `'ready'` event `()`

Event emitted when all events for initial results data have been emitted.
Later events are about changes to results data.

##### `'refresh'` event `()`

Event emitted when a refresh has finished and all events for changes to results data
as part of one refresh have been emitted.

##### `'insert'` event `(row)`

Event emitted when a new row has been added to the reactive query.
`row` is provided as an argument. In `id` mode, `row` contains only the value
of the unique column of the row which has been inserted. In `changed` and `full` modes,
`row` contains full row which has been inserted.

##### `'update'` event `(row, columns)`

Event emitted when a row has been updated.
`row` is provided as an argument. In `id` mode, `row` contains only the value
of the unique column of the row which has been updated. In `changed` mode, `row`
contains also data for columns which have changed. In `full` mode, `row`
contains the full updated row, both columns which have changed and those which have not.
`columns` is a list of columns which have changed.

##### `'delete'` event `(row)`

Event emitted when a row has been deleted. `row` is provided as an argument and
contains only the unique column of the row which has been deleted.

##### `'error'` event `(error)`

Event emitted when there is an error at the reactive query level.
`error` is provided as an argument.

##### `'stop'` event `([error])`

Event emitted when the reactive query stops. If it stopped because of an error,
the `error` is provided as an argument.

#### `Readable` stream API

There are more methods, properties, and options available through the
[`Readable` stream base class](https://nodejs.org/api/stream.html#stream_readable_streams).

When operating as a stream, the reactive handle is producing objects
describing a change to the reactive query. They are of the following
structure:

```js
{
  op: <event name>,
  ... <payload> ...
}
```

`event name` matches names of events emitted when operating as an event emitter.
Arguments of those events are converted to object's payload.

Stream supports backpressure and if consumer of the stream reads slower than
what `refreshThrottleWait` dictates should be the minimal delay between refreshes,
further refreshing is paused until stream is drained.

##### `read([size])`

Reads the next object describing a change to the reactive query, if available.
[More information](https://nodejs.org/api/stream.html#stream_readable_read_size).

##### `pipe(destination[, options])`

The method attaches the stream to the `destination`, using `options`.
[More information](https://nodejs.org/api/stream.html#stream_readable_pipe_destination_options).

##### `destroy([error])`

Destroy the stream and stop the reactive query. It emits `'error'` (if `error` argument
is provided, which is then passed as payload in `'error'` event) and `'close'` events.
[More information](https://nodejs.org/api/stream.html#stream_readable_destroy_error).

##### 'data' event `(chunk)`

Event emitted whenever the stream is relinquishing ownership of a chunk
of data to a consumer.
`chunk` is provided as an argument.
[More information](https://nodejs.org/api/stream.html#stream_event_data).

##### 'readable' event `()`

Event emitted when there is data available to be read from the stream.
[More information](https://nodejs.org/api/stream.html#stream_event_readable).

##### 'error' event `(error)`

Event emitted when there is an error at the reactive query level.
`error` is provided as an argument.
[More information](https://nodejs.org/api/stream.html#stream_event_error_1).

##### 'close' event `()`

Event emitted when the stream has been destroyed.
[More information](https://nodejs.org/api/stream.html#stream_event_close_1).

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
