const assert = require('assert');
const EventEmitter = require('events');
const {Readable} = require('stream');

const {Client} = require('pg');

const {randomId} = require('./random');

const DEFAULT_QUERY_OPTIONS = {
  // TODO: Allow multi-column unique index as well.
  uniqueColumn: 'id',
  refreshThrottleWait: 100, // ms
  mode: 'id',
  // TODO: Should batch size be on the number of rows to fetch and not number of changes?
  batchSize: 0,
  types: null,
};

class ReactiveQueryHandle extends Readable {
  constructor(manager, client, queryId, query, options) {
    super({
      autoDestroy: options.autoDestroy,
      // We disable internal buffering in "Readable" because we buffer ourselves.
      // We want to detect backpressure as soon as possible so that we do not refresh unnecessary.
      highWaterMark: 0,
      objectMode: true,
    });

    this.manager = manager;
    this.client = client;
    this.queryId = queryId;
    this.query = query;
    this.options = options;

    this._isStream = false;
    this._started = false;
    this._stopped = false;
    this._dirty = false;
    this._batch = [];
    this._readyPending = false;
    this._sources = null;
    this._throttleTimestamp = 0;
    this._throttleTimeout = null;
    this._streamQueue = [];
    this._streamPaused = false;
    this._sourceChangedPending = false;
  }

  async start() {
    if (this._started) {
      throw new Error("Query has already been started.");
    }
    if (this._stopped) {
      throw new Error("Query has already been stopped.");
    }

    this._started = true;

    try {
      const {rows: queryExplanation} = await this.client.query({
        text: `EXPLAIN (FORMAT JSON) (${this.query})`,
        rowMode: 'array',
      });

      this._sources = [...this._extractSources(queryExplanation)].sort();

      // TODO: Handle also TRUNCATE of sources?
      //       What if it is not really a table but a view or something else?
      //       We should try to create a trigger inside a sub-transaction we can rollback if it fails.
      const sourcesTriggers = this._sources.map((source) => {
        return `
          CREATE TRIGGER "${this.queryId}_source_changed_${source}_insert" AFTER INSERT ON "${source}" REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_source_changed('${this.queryId}');
          CREATE TRIGGER "${this.queryId}_source_changed_${source}_update" AFTER UPDATE ON "${source}" REFERENCING NEW TABLE AS new_table OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_source_changed('${this.queryId}');
          CREATE TRIGGER "${this.queryId}_source_changed_${source}_delete" AFTER DELETE ON "${source}" REFERENCING OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_source_changed('${this.queryId}');
        `;
      }).join('\n');

      await this.manager.client.query(`
        START TRANSACTION;
        LISTEN "${this.queryId}_query_ready";
        LISTEN "${this.queryId}_query_changed";
        LISTEN "${this.queryId}_query_refreshed";
        LISTEN "${this.queryId}_source_changed";
        COMMIT;
      `);

      // We create a temporary materialized view based on a query, but without data, just
      // structure. We add triggers to notify the client about changes to sources and
      // the materialized view. We then refresh the materialized view for the first time.
      await this.client.query(`
        START TRANSACTION;
        CREATE TEMPORARY MATERIALIZED VIEW "${this.queryId}_view" AS ${this.query} WITH NO DATA;
        CREATE UNIQUE INDEX "${this.queryId}_view_id" ON "${this.queryId}_view" ("${this.options.uniqueColumn}");
        ${sourcesTriggers}
        CREATE TRIGGER "${this.queryId}_query_changed_insert" AFTER INSERT ON "${this.queryId}_view" REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_query_changed('${this.queryId}', '${this.options.uniqueColumn}');
        CREATE TRIGGER "${this.queryId}_query_changed_update" AFTER UPDATE ON "${this.queryId}_view" REFERENCING NEW TABLE AS new_table OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_query_changed('${this.queryId}', '${this.options.uniqueColumn}');
        CREATE TRIGGER "${this.queryId}_query_changed_delete" AFTER DELETE ON "${this.queryId}_view" REFERENCING OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_query_changed('${this.queryId}', '${this.options.uniqueColumn}');
        REFRESH MATERIALIZED VIEW CONCURRENTLY "${this.queryId}_view";
        NOTIFY "${this.queryId}_query_refreshed", '{}';
        NOTIFY "${this.queryId}_query_ready", '{}';
        COMMIT;
      `);
    }
    catch (error) {
      this._stopped = true;
      if (!this._isStream) {
        this.emit('error', error);
        this.emit('stop', error);
      }
      throw error;
    }

    if (!this._isStream) {
      this.emit('start');
    }
  }

  async stop(error) {
    if (this._isStream) {
      await new Promise((resolve, reject) => {
        this.once('close', resolve);
        this.once('error', reject);
        this.destroy(error);
      });
    }
    else {
      await this._stop(error);
    }
  }

  async _stop(error) {
    if (this._stopped) {
      return;
    }
    if (!this._started) {
      throw new Error("Query has not been started.");
    }

    this._stopped = true;

    try {
      const sourcesTriggers = this._sources.map((source) => {
        return `
          DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_insert" ON "${source}";
          DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_update" ON "${source}";
          DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_delete" ON "${source}";
        `;
      }).join('\n');

      await this.manager.client.query(`
        START TRANSACTION;
        UNLISTEN "${this.queryId}_query_ready";
        UNLISTEN "${this.queryId}_query_changed";
        UNLISTEN "${this.queryId}_query_refreshed";
        UNLISTEN "${this.queryId}_source_changed";
        COMMIT;
      `);

      await this.client.query(`
        START TRANSACTION;
        ${sourcesTriggers}
        DROP MATERIALIZED VIEW IF EXISTS "${this.queryId}_view" CASCADE;
        COMMIT;
      `);
    }
    catch (error) {
      if (!this._isStream) {
        this.emit('error', error);
        this.emit('stop', error);
      }
      throw error;
    }

    if (!this._isStream) {
      this.emit('stop', error);
    }
  }

  async _onQueryReady(payload) {
    if (!this._started || this._stopped) {
      return;
    }

    this._readyPending = true;
  }

  async _onQueryRefreshed(payload) {
    if (!this._started || this._stopped) {
      return;
    }

    if (this._dirty) {
      this._dirty = false;
      await this._processBatch(true);
      if (this._isStream) {
        this._streamPush({op: 'refresh'});
      }
      else {
        this.emit('refresh');
      }
      if (this._readyPending) {
        this._readyPending = false;
        if (this._isStream) {
          this._streamPush({op: 'ready'});
        }
        else {
          this.emit('ready');
        }
      }
    }
  }

  async _onQueryChanged(payload) {
    if (!this._started || this._stopped) {
      return;
    }

    if (payload.op === 'insert' || payload.op === 'update' || payload.op === 'delete') {
      this._dirty = true;
      this._batch.push(payload);
      await this._processBatch(false);
    }
    else {
      console.error(`Unknown query changed op '${payload.op}'.`);
    }
  }

  async _processBatch(isRefresh) {
    // When only ID is requested we do not need to fetch data, so we do not batch anything.
    if (this.options.mode === 'id') {
      this._processBatchIdMode();
    }
    else if (this.options.batchSize < 1 && isRefresh) {
      await this._processBatchFetchMode();
    }
    else if (this.options.batchSize > 0 && this._batch.length >= this.options.batchSize) {
      await this._processBatchFetchMode();
    }
  }

  async flush() {
    if (this._stopped) {
      // This method is not returning anything, so we just ignore the call.
      return;
    }
    if (!this._started) {
      throw new Error("Query has not been started.");
    }

    if (this.options.mode === 'id') {
      this._processBatchIdMode();
    }
    else {
      await this._processBatchFetchMode();
    }
  }

  _processBatchIdMode() {
    while (this._batch.length) {
      const payload = this._batch.shift();

      const row = {};
      row[this.options.uniqueColumn] = payload.id;

      if (this._isStream) {
        delete payload.id;
        payload.row = row;
      }

      if (payload.op === 'insert') {
        if (this._isStream) {
          this._streamPush(payload);
        }
        else {
          this.emit('insert', row);
        }
      }
      else if (payload.op === 'update') {
        if (this._isStream) {
          this._streamPush(payload);
        }
        else {
          this.emit('update', row, payload.columns);
        }
      }
      else if (payload.op === 'delete') {
        if (this._isStream) {
          this._streamPush(payload);
        }
        else {
          this.emit('delete', row);
        }
      }
      else {
        assert.fail(`Unexpected query changed op '${payload.op}'.`);
      }
    }
  }

  async _processBatchFetchMode() {
    if (!this._batch.length) {
      return;
    }

    // Copy to a local variable.
    const batch = this._batch;
    this._batch = [];

    const idsToFetchForInsert = new Set();
    const idsToFetchForUpdate = new Set();
    const columnsToFetchForUpdate = new Set();
    for (const payload of batch) {
      if (payload.op === 'insert') {
        idsToFetchForInsert.add(payload.id);
      }
      else if (payload.op === 'update') {
        idsToFetchForUpdate.add(payload.id);
        for (const columnName of payload.columns) {
          columnsToFetchForUpdate.add(columnName);
        }
      }
    }

    // Always fetch ID column.
    columnsToFetchForUpdate.add(this.options.uniqueColumn);

    // "idsToFetchForInsert" and "idsToFetchForUpdate" should be disjoint and
    // this is true because we are fetching inside one refresh.

    const rows = new Map();
    if (this.options.mode === 'changed') {
      const updateColumns = '"' + [...columnsToFetchForUpdate].join('", "') + '"';

      // We do one query for inserted rows.
      let response = await this.client.query({
        text: `
          SELECT * FROM "${this.queryId}_view" WHERE "${this.options.uniqueColumn}"=ANY($1);
        `,
        values: [[...idsToFetchForInsert]],
        types: this.options.types,
      });

      for (const row of response.rows) {
        rows.set(row[this.options.uniqueColumn], row);
      }

      // And we do one query for updated rows. Here we might be able to fetch less columns.
      response = await this.client.query({
        text: `
          SELECT ${updateColumns} FROM "${this.queryId}_view" WHERE "${this.options.uniqueColumn}"=ANY($1);
        `,
        values: [[...idsToFetchForUpdate]],
        types: this.options.types,
      });

      for (const row of response.rows) {
        rows.set(row[this.options.uniqueColumn], row);
      }
    }
    else if (this.options.mode === 'full') {
      // We can just do one query.
      const response = await this.client.query({
        text: `
          SELECT * FROM "${this.queryId}_view" WHERE "${this.options.uniqueColumn}"=ANY($1);
        `,
        values: [[...idsToFetchForInsert, ...idsToFetchForUpdate]],
        types: this.options.types,
      });

      for (const row of response.rows) {
        rows.set(row[this.options.uniqueColumn], row);
      }
    }
    else {
      assert.fail(`Unexpected query mode '${this.options.mode}'.`);
    }

    for (const payload of batch) {
      if (payload.op === 'insert') {
        const row = rows.get(payload.id);
        // It could happen that row was deleted in meantime and we could not fetch it.
        // We do not do anything for now and leave it to a future notification to handle this.
        if (row) {
          if (this._isStream) {
            delete payload.id;
            payload.row = row;
            this._streamPush(payload);
          }
          else {
            this.emit('insert', row);
          }
        }
      }
      else if (payload.op === 'update') {
        const row = rows.get(payload.id);
        // It could happen that row was deleted in meantime and we could not fetch it.
        // We do not do anything for now and leave it to a future notification to handle this.
        if (row) {
          // Different rows might have different columns updated. We had to fetch
          // a superset of all changed columns and here we have to remove those
          // columns which have not changed for a particular row.
          for (const key of Object.keys(row)) {
            if (key !== this.options.uniqueColumn && !payload.columns.includes(key)) {
              delete row[key];
            }
          }
          if (this._isStream) {
            delete payload.id;
            payload.row = row;
            this._streamPush(payload);
          }
          else {
            this.emit('update', row, payload.columns);
          }
        }
      }
      else if (payload.op === 'delete') {
        const row = {};
        row[this.options.uniqueColumn] = payload.id;
        if (this._isStream) {
          delete payload.id;
          payload.row = row;
          this._streamPush(payload);
        }
        else {
          this.emit('delete', row);
        }
      }
      else {
        assert.fail(`Unexpected query changed op '${payload.op}'.`);
      }
    }
  }

  async refresh() {
    if (this._stopped) {
      // This method is not returning anything, so we just ignore the call.
      return;
    }
    if (!this._started) {
      throw new Error("Query has not been started.");
    }

    this.client.query(`
      START TRANSACTION;
      REFRESH MATERIALIZED VIEW CONCURRENTLY "${this.queryId}_view";
      NOTIFY "${this.queryId}_query_refreshed", '{}';
      COMMIT;
    `);
  }

  _onSourceChanged(payload) {
    if (!this._started || this._stopped) {
      return;
    }

    const timestamp = new Date().valueOf();
    if (!this._throttleTimestamp) {
      this._throttleTimestamp = timestamp;
    }

    // If stream is paused or if queue is not empty, we set a flag to
    // postpone processing this source changed event. We retry once
    // stream is not paused anymore or once we emptied the queue.
    if (this._streamPaused || this._streamQueue.length) {
      this._sourceChangedPending = true;
      return;
    }

    const remaining = this.options.refreshThrottleWait - (timestamp - this._throttleTimestamp);
    if (remaining <= 0 || remaining > this.options.refreshThrottleWait) {
      if (this._throttleTimeout) {
        clearTimeout(this._throttleTimeout);
        this._throttleTimeout = null;
      }
      this._throttleTimestamp = timestamp;
      this.refresh().catch((error) => {
        this.emit('error', error);
      });
    }
    else if (!this._throttleTimeout) {
      this._throttleTimeout = setTimeout(async () => {
        this._throttleTimestamp = 0;
        this._throttleTimeout = null;
        this.refresh().catch((error) => {
          this.emit('error', error);
        });
      }, remaining);
    }
  }

  _extractSources(queryExplanation) {
    let sources = new Set();

    if (Array.isArray(queryExplanation)) {
      for (const element of queryExplanation) {
        sources = new Set([...sources, ...this._extractSources(element)]);
      }
    }
    else if (queryExplanation instanceof Object) {
      for (const [key, value] of Object.entries(queryExplanation)) {
        if (key === 'Relation Name') {
          sources.add(value);
        }
        else {
          sources = new Set([...sources, ...this._extractSources(value)]);
        }
      }
    }

    return sources;
  }

  _read(size) {
    (async () => {
      // We can resume the stream now. This does not necessary mean we also start processing
      // source changed events again, because the queue might not yet be empty.
      this._resumeStream();
      if (!this._started) {
        this._isStream = true;
        await this.start();
      }
      // We try to get the queue to be empty. If this happens, we will retry processing
      // a source changed event (because stream is also not paused anymore),
      // if it happened in the past.
      this._streamFlush();
    })().catch((error) => {
      this.emit('error', error);
    });
  }

  _destroy(error, callback) {
    (async () => {
      await this._stop();
    })().catch((error) => {
      callback(error);
    }).then(() => {
      callback(error);
    });
  }

  _streamPush(data) {
    this._streamQueue.push(data);
    this._streamFlush();
  }

  _streamFlush() {
    while (!this._streamPaused && this._streamQueue.length) {
      const chunk = this._streamQueue.shift();
      if (!this.push(chunk)) {
        this._pauseStream();
      }
    }

    if (!this._streamPaused) {
      // Stream is not paused which means the queue is empty. We retry processing a
      // source changed event, if it happened in the past but we postponed it.
      // We reuse "_resumeStream" for this purpose.
      this._resumeStream();
    }
  }

  _pauseStream() {
    this._streamPaused = true;
    if (this._throttleTimeout) {
      clearTimeout(this._throttleTimeout);
      this._throttleTimeout = null;
    }
  }

  _resumeStream() {
    this._streamPaused = false;
    // Stream is not paused anymore, retry processing a source
    // changed event, if it happened while we were paused.
    if (this._sourceChangedPending) {
      this._sourceChangedPending = false;
      this._onSourceChanged(null);
    }
  }
}

const DEFAULT_MANAGER_OPTIONS = {
  maxConnections: 10,
  connectionConfig: {},
  handleClass: ReactiveQueryHandle,
};

const NOTIFICATION_REGEX = /^(.+)_(query_ready|query_changed|query_refreshed|source_changed)$/;

// TODO: Disconnect idle clients after some time.
//       Idle meaning that they do not have any reactive queries using them.
class Manager extends EventEmitter {
  constructor(options={}) {
    super();

    this.options = Object.assign({}, DEFAULT_MANAGER_OPTIONS, options);

    if (!(this.options.maxConnections > 0)) {
      throw new Error("\"maxConnections\" option has to be larger than 0.")
    }

    // Manager's client used for notifications. Queries use their
    // own set of clients from a client pool, "this._clients".
    this.client = null;

    this._started = false;
    this._stopped = false;
    // Map between a "queryId" and a reactive query handle.
    this._handlesForQuery = new Map();
    // Map between a client and a number of reactive queries using it.
    this._clients = new Map();
    this._pendingClients = [];
  }

  async start() {
    if (this._started) {
      throw new Error("Manager has already been started.");
    }
    if (this._stopped) {
      throw new Error("Manager has already been stopped.");
    }

    this._started = true;

    try {
      this.client = new Client(this.options.connectionConfig);

      this.client.on('error', (error) => {
        this.emit('error', error, this.client);
      });
      this.client.on('end', () => {
        this.emit('disconnect', this.client);
      });
      this.client.on('notification', (message) => {
        this._onNotification(message);
      });

      await this.client.connect();

      this.emit('connect', this.client);

      await this.client.query(`
        CREATE EXTENSION IF NOT EXISTS hstore;
      `);
    }
    catch (error) {
      this._stopped = true;
      this.emit('error', error);
      this.emit('stop', error);
      throw error;
    }

    this.emit('start');
  }

  async stop(error) {
    if (this._stopped) {
      return;
    }
    if (!this._started) {
      throw new Error("Manager has not been started.");
    }

    this._stopped = true;

    try {
      while (this._handlesForQuery.size) {
        for (const [queryId, handle] of this._handlesForQuery.entries()) {
          // "stop" triggers "end" callback which removes the handle.
          await handle.stop();
        }
      }

      // They should all be removed now through "end" callbacks when we
      // called "stop" on all handles.
      assert(this._handlesForQuery.size === 0, "\"handlesForQuery\" should be empty.");

      // Disconnect all clients.
      while (this._clients.size) {
        for (const [client, utilization] of this._clients.entries()) {
          assert(utilization === 0, "Utilization of a client should be zero.");

          await client.end();
          this._clients.delete(client);
        }
      }

      await this.client.end();
    }
    catch (error) {
      this.emit('error', error);
      this.emit('stop', error);
      throw error;
    }

    this.emit('stop', error);
  }

  async _getClient() {
    if (this._stopped) {
      // This method is returning a client, so we throw.
      throw new Error("Manager has been stopped.");
    }
    if (!this._started) {
      throw new Error("Manager has not been started.");
    }

    // If we do not yet have all connections open, make a new one now.
    if (this._clients.size + this._pendingClients.length < this.options.maxConnections) {
      const clientPromise = this._createClient();
      clientPromise.then((client) => {
        this._clients.set(client, 0);
        const index = this._pendingClients.indexOf(clientPromise);
        if (index >= 0) {
          this._pendingClients.splice(index, 1);
        }
      });
      this._pendingClients.push(clientPromise);
    }

    await Promise.all(this._pendingClients);

    assert(this._clients.size > 0, "\"maxConnections\" has to be larger than 0.");

    // Find clients with the least number of active queries. We want
    // to distribute all queries between all clients equally.
    let availableClients = [];
    let lowestUtilization = Number.POSITIVE_INFINITY;
    for (const [client, utilization] of this._clients.entries()) {
        if (utilization < lowestUtilization) {
          lowestUtilization = utilization;
          availableClients = [client];
        }
        else if (utilization === lowestUtilization) {
          availableClients.push(client);
        }
    }

    // We pick a random client from available clients.
    return availableClients[Math.floor(Math.random() * availableClients.length)];
  }

  _setHandleForQuery(handle, queryId) {
    this._handlesForQuery.set(queryId, handle);
  }

  _getHandleForQuery(queryId) {
    return this._handlesForQuery.get(queryId);
  }

  _deleteHandleForQuery(queryId) {
    this._handlesForQuery.delete(queryId);
  }

  _useClient(client) {
    this._clients.set(client, this._clients.get(client) + 1);
  }

  _releaseClient(client) {
    this._clients.set(client, this._clients.get(client) - 1);
  }

  async _createClient() {
    const client = new Client(this.options.connectionConfig);

    client.on('error', (error) => {
      this.emit('error', error, client);
    });
    client.on('end', () => {
      this.emit('disconnect', client);
    });

    await client.connect();

    // We define functions as temporary for every client so that triggers using
    // them are dropped when the client disconnects (session ends).
    // We can use INNER JOIN in "notify_query_changed" because we know that column names
    // are the same in "new_table" and "old_table", and we are joining on column names.
    // There should always be something different, so "array_agg" should never return NULL
    // in "notify_query_changed", but we still make sure this is so with COALESCE.
    // TODO: We could have only one set of "notify_source_changed" triggers per each source.
    //       We could install them the first time but then leave them around. Clients subscribing
    //       to shared notifications based on source name.
    await client.query(`
      CREATE OR REPLACE FUNCTION pg_temp.notify_query_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
        DECLARE
          query_id TEXT := TG_ARGV[0];
          primary_column TEXT := TG_ARGV[1];
        BEGIN
          IF (TG_OP = 'INSERT') THEN
            EXECUTE 'SELECT pg_notify(''' || query_id || '_query_changed'', row_to_json(changes)::text) FROM (SELECT ''insert'' AS op, "' || primary_column || '" AS id FROM new_table) AS changes';
          ELSIF (TG_OP = 'UPDATE') THEN
            EXECUTE 'SELECT pg_notify(''' || query_id || '_query_changed'', row_to_json(changes)::text) FROM (SELECT ''update'' AS op, new_table."' || primary_column || '" AS id, (SELECT COALESCE(array_agg(row1.key), ARRAY[]::TEXT[]) FROM each(hstore(new_table)) AS row1 INNER JOIN each(hstore(old_table)) AS row2 ON (row1.key=row2.key) WHERE row1.value IS DISTINCT FROM row2.value) AS columns FROM new_table JOIN old_table ON (new_table."' || primary_column || '"=old_table."' || primary_column || '")) AS changes';
          ELSIF (TG_OP = 'DELETE') THEN
            EXECUTE 'SELECT pg_notify(''' || query_id || '_query_changed'', row_to_json(changes)::text) FROM (SELECT ''delete'' AS op, "' || primary_column || '" AS id FROM old_table) AS changes';
          END IF;
          RETURN NULL;
        END
      $$;
      CREATE OR REPLACE FUNCTION pg_temp.notify_source_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
        DECLARE
          query_id TEXT := TG_ARGV[0];
        BEGIN
          IF (TG_OP = 'INSERT') THEN
            PERFORM * FROM new_table LIMIT 1;
            IF FOUND THEN
              EXECUTE 'NOTIFY "' || query_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
            END IF;
          ELSIF (TG_OP = 'UPDATE') THEN
            PERFORM * FROM ((TABLE new_table EXCEPT TABLE new_table) UNION ALL (TABLE new_table EXCEPT TABLE old_table)) AS differences LIMIT 1;
            IF FOUND THEN
              EXECUTE 'NOTIFY "' || query_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
            END IF;
          ELSIF (TG_OP = 'DELETE') THEN
            PERFORM * FROM old_table LIMIT 1;
            IF FOUND THEN
              EXECUTE 'NOTIFY "' || query_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
            END IF;
          ELSIF (TG_OP = 'TRUNCATE') THEN
            EXECUTE 'NOTIFY "' || query_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
          END IF;
          RETURN NULL;
        END
      $$;
    `);

    this.emit('connect', client);

    return client;
  }

  async query(query, options={}) {
    if (this._stopped) {
      // This method is returning a handle, so we throw.
      throw new Error("Manager has been stopped.");
    }
    if (!this._started) {
      throw new Error("Manager has not been started.");
    }

    options = Object.assign({}, DEFAULT_QUERY_OPTIONS, options);

    // We generate ID outside of the constructor so that it can be async.
    const queryId = await randomId();
    const client = await this._getClient();

    const handle = new this.options.handleClass(this, client, queryId, query, options);
    this._setHandleForQuery(handle, queryId);
    this._useClient(client);
    handle.once('stop', (error) => {
      this._deleteHandleForQuery(queryId);
      this._releaseClient(client);
    });
    handle.once('close', () => {
      this._deleteHandleForQuery(queryId);
      this._releaseClient(client);
    });
    return handle;
  }

  _onNotification(message) {
    if (!this._started || this._stopped) {
      return;
    }

    const channel = message.channel;

    const match = NOTIFICATION_REGEX.exec(channel);
    if (!match) {
      return;
    }

    const queryId = match[1];
    const notificationType = match[2];

    const handle = this._getHandleForQuery(queryId);
    if (!handle) {
      console.warn(`QueryId '${queryId}' without a handle.`);
      return;
    }

    const payload = JSON.parse(message.payload);

    if (notificationType === 'query_ready') {
      handle._onQueryReady(payload).catch((error) => {
        this.emit('error', error);
      });
    }
    else if (notificationType === 'query_changed') {
      handle._onQueryChanged(payload).catch((error) => {
        this.emit('error', error);
      });
    }
    else if (notificationType === 'query_refreshed') {
      handle._onQueryRefreshed(payload).catch((error) => {
        this.emit('error', error);
      });
    }
    else if (notificationType === 'source_changed') {
      handle._onSourceChanged(payload);
    }
    else {
      console.error(`Unknown notification type '${notificationType}'.`);
    }
  }
}

module.exports = {
  Manager,
  ReactiveQueryHandle,
};
