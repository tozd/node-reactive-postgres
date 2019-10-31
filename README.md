# reactive-postgres

This node.js package brings reactive (or live) queries to PostgreSQL. You can take an arbitrary `SELECT` query,
using multiple joins, data transformations, and even custom functions, and besides the initial set of
results also get real-time updates about any changes to those results. This can enable you to keep UI in sync
with the database in a reactive manner.

## Installation

This is a node.js package. You can install it using NPM:

```bash
$ npm install reactive-postgres
```

This package uses PostgreSQL. See [documentation](https://www.postgresql.org/docs/devel/tutorial-start.html)
for more information how to install and use it.

Requires PostgreSQL 11 or newer.

## reactive-postgres for enterprise

Available as part of the Tidelift Subscription.

The maintainers of reactive-postgres and thousands of other packages are working with Tidelift to deliver commercial support and maintenance for the open source dependencies you use to build your applications. Save time, reduce risk, and improve code health, while paying the maintainers of the exact dependencies you use. [Learn more.](https://tidelift.com/subscription/pkg/npm-reactive-postgres?utm_source=npm-reactive-postgres&utm_medium=referral&utm_campaign=enterprise&utm_term=repo)

## Usage

As an event emitter:

```js
const {Manager} = require('reactive-postgres');

const manager = new Manager({
  connectionConfig: {
    user: 'dbuser',
    host: 'database.server.com',
    database: 'mydb',
    password: 'secretpassword',
    port: 3211,
  },
});

(async () => {
  await manager.start();

  const handle = await manager.query(`SELECT * FROM posts`, {
    uniqueColumn: 'id',
    mode: 'changed',
  });

  handle.on('start', () => {
    console.log("query has started");
  });

  handle.on('ready', () => {
    console.log("initial data have been provided");
  });

  handle.on('refresh', () => {
    console.log("all query changes have been provided");
  });

  handle.on('insert', (row) => {
    console.log("row inserted", row);
  });

  handle.on('update', (row, columns) => {
    console.log("row updated", row, columns);
  });

  handle.on('delete', (row) => {
    console.log("row deleted", row);
  });

  handle.on('error', (error) => {
    console.error("query error", error);
  });

  handle.on('stop', (error) => {
    console.log("query has stopped", error);
  });

  handle.start();
})().catch((error) => {
  console.error("async error", error);
});

process.on('SIGINT', () => {
  manager.stop().catch((error) => {
    console.error("async error", error);
  }).finally(() => {
    process.exit();
  });
});
```

As a stream:

```js
const {Manager} = require('reactive-postgres');
const through2 = require('through2');

const manager = new Manager({
  connectionConfig: {
    user: 'dbuser',
    host: 'database.server.com',
    database: 'mydb',
    password: 'secretpassword',
    port: 3211,
  },
});

const jsonStream = through2.obj(function (chunk, encoding, callback) {
  this.push(JSON.stringify(chunk, null, 2) + '\n');
  callback();
});

(async () => {
  await manager.start();

  const handle = await manager.query(`SELECT * FROM posts`, {
    uniqueColumn: 'id',
    mode: 'changed',
  });

  handle.on('error', (error) => {
    console.error("stream error", error);
  });

  handle.on('close', () => {
    console.log("stream has closed");
  });

  handle.pipe(jsonStream).pipe(process.stdout);
})().catch((error) => {
  console.error("async error", error);
});

process.on('SIGINT', () => {
  manager.stop().catch((error) => {
    console.error("async error", error);
  }).finally(() => {
    process.exit();
  });
});
```


## Design

Reactive queries are implemented in the following manner:

* For every reactive query, a `TEMPORARY TABLE` is created in the database
  which serves as cache for query results.
* Moreover, the query is `PREPARE`d, so that it does not have to be parsed again
  and again.
* Triggers are added to all query sources for the query, so that
  when any of sources change, this package is notified using
  `LISTEN`/`NOTIFY` that a source has changed, which can
  potentially influence the results of the query.
* Package waits for source changed events, and throttles them based
  on `refreshThrottleWait` option. Once the delay expires, the package
  creates a new temporary table with new query results. It compares
  the old and new table and computes changes using another database query.
* Changes are returned the package's client and exposed to the user.
* The old temporary table is dropped and the new one is renamed to take
  its place.

## Performance

* Memory use of node.js process is
  [very low, is constant and does not grow with the number of rows in query results](https://mitar.github.io/node-pg-reactivity-benchmark/viewer.html?results/reactive-postgres-id.json).
  Moreover, node.js process also does no heavy computation
  and mostly just passes data around. All this is achieved by caching a query using a
  temporary table in the database instead of the client, and using a database query
  to compare new and old query results.
* Computing changes is done through one query, a very similar query to the one used
  internally by `REFRESH MATERIALIZED VIEW CONCURRENTLY` PostgreSQL command.
* Based on the design, the time to compute changes and provide them to the client
  seems to be the lowest when compared with other similar packages. For more
  information about performance comparisons of this package and related packages,
  see [this benchmark tool](https://github.com/mitar/node-pg-reactivity-benchmark) and
  [results at the end](https://github.com/mitar/node-pg-reactivity-benchmark#results).
* Because this package uses temporary tables, consider increasing
  [`temp_buffers`](https://www.postgresql.org/docs/devel/runtime-config-resource.html#GUC-TEMP-BUFFERS)
  PostgreSQL configuration so that there is more space for temporary tables in memory.
  Consider [creating a dedicated tablespace](https://www.postgresql.org/docs/9.4/sql-createtablespace.html)
  and configuring [`temp_tablespaces`](https://www.postgresql.org/docs/devel/runtime-config-client.html#GUC-TEMP-TABLESPACES).
* You might consider increasing `refreshThrottleWait` for reactive queries for
  which you can tolerate lower refresh rate and higher update latency, to decrease
  load on the database. Making too many refreshes for complex queries can saturate
  the database which then leads to even higher delays. Paradoxically, having higher
  `refreshThrottleWait` could give you lower delay in comparison with a saturated state.
* Multiple reactive queries share the same connection to the database.
  So correctly configuring `maxConnections` is important. More connections there are,
  higher load is on the database, but over more connections reactive queries can spread.
  But higher load on the database can lead to its saturation.
* Currently, when any of sources change in any manner, whole query is rerun
  and results compared with cached results (after a throttling delay).
  To improve this, refresh could be done only when it is known that a source change
  is really influencing the results. Ideally, we could even compute changes to
  results directly based on changes to sources. See
  [#7](https://github.com/tozd/node-reactive-postgres/issues/7) for more information.

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
* `mode`, default `changed`: in which mode to operate, it can be:
  * `columns`: for every query results change, provide only which row and columns changed
  * `changed`: for every query results change, provide new values for changed columns, too
  * `full`: for every query results change, provide full changed rows,
    both columns which have changed and those which have not
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
`row` is provided as an argument. In `columns` mode, `row` contains only the value
of the unique column of the row which has been inserted. In `changed` and `full` modes,
`row` contains full row which has been inserted.

##### `'update'` event `(row, columns)`

Event emitted when a row has been updated.
`row` is provided as an argument. In `columns` mode, `row` contains only the value
of the unique column of the row which has been updated. In `changed` mode, `row`
contains also data for columns which have changed. In `full` mode, `row`
contains the full updated row, both columns which have changed and those which have not.
`columns` is a list of columns which have changed.

##### `'delete'` event `(row)`

Event emitted when a row has been deleted. `row` is provided as an argument.
In `columns` mode, `row` contains only the value
of the unique column of the row which has been deleted. In `changed` and `full` modes,
`row` contains full row which has been deleted.

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
  query to obtain updated rows, which are identified through hashes of their full content and client then computes changes,
  on the other hand, this package maintains cached results of the query in a temporary table in the database
  and just compares new results with cached results and return updates to the client
* [pg-live-query](https://github.com/nothingisdead/pg-live-query) – adds revision columns to sources and additional
  temporary table to store latest revisions for results, moreover, it rewrites queries to expose those additional
  revision columns, all this then allows simpler determination of what has changed when the query is refreshed,
  inside the database through a query, but in benchmarks it seems all this additional complexity does not really
  make things faster in comparison with just comparing new results with cached results directly, which is what this
  package does
* [pg-query-observer](https://github.com/Richie765/pg-query-observer) – it seems like a bit cleaned and updated version
  of `pg-live-select`, but buggy and does not work with multiple parallel queries
* [pg-reactivity-benchmark](https://github.com/mitar/node-pg-reactivity-benchmark) – a benchmark for this and above
  mentioned packages
