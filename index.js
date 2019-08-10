const assert = require('assert');
const EventEmitter = require('events');
const {Readable} = require('stream');

const AwaitLock = require('await-lock');
const {Client} = require('pg');
const {randomId} = require('@tozd/random-id');

const DEFAULT_QUERY_OPTIONS = {
  // TODO: Allow multi-column unique index as well.
  uniqueColumn: 'id',
  refreshThrottleWait: 100, // ms
  mode: 'changed',
  types: null,
};

const OP_MAP = new Map([
  [1, 'insert'],
  [2, 'update'],
  [3, 'delete'],
]);

// TODO: Should we expose an event that the source has changed?
//       We might not want this because it is an internal detail. Once we move to
//       something like Incremental View Maintenance there will be no such event possible.
// TODO: Should we allow disabling automatic refresh?
//       This could allow one to then provide custom logic for refresh.
//       We could use a negative value of "refreshThrottleWait" for this.
//       But how would the user know when to refresh without an event that source has changed?
class ReactiveQueryHandle extends Readable {
  constructor(manager, client, queryId, query, options) {
    super({
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
    this._sources = null;
    this._throttleTimestamp = 0;
    this._throttleTimeout = null;
    this._streamQueue = [];
    this._streamPaused = false;
    this._sourceChangedPending = false;
    this._refreshInProgress = false;
  }

  async start() {
    if (this._started) {
      throw new Error("Query has already been started.");
    }
    if (this._stopped) {
      throw new Error("Query has already been stopped.");
    }

    this._started = true;

    let queryExplanation;
    await this.client.lock.acquireAsync();
    try {
      const results = await this.client.query({
        text: `
          PREPARE "${this.queryId}_query" AS (${this.query});
          EXPLAIN (FORMAT JSON) EXECUTE "${this.queryId}_query";
        `,
        rowMode: 'array',
      });

      queryExplanation = results[1].rows;
    }
    catch (error) {
      this._stopped = true;

      if (!this._isStream) {
        this.emit('error', error);
        this.emit('stop', error);
      }

      throw error;
    }
    finally {
      await this.client.lock.release();
    }

    this._sources = [...this._extractSources(queryExplanation)].sort();

    // We are just starting, how could it be true?
    assert(!this._refreshInProgress);
    // We set the flag so that any source changed event gets postponed.
    this._refreshInProgress = true;

    try {
      // We create triggers for each source in its own transaction instead of all of them
      // at once so that we do not have to lock all sources at the same time and potentially
      // introduce a deadlock if any of those sources is also used in some other materialized
      // view at the same time and it is just being refreshed. This means that source changed
      // events could already start arriving before we have a materialized view created,
      // but we have them postponed by setting "_refreshInProgress" to "true".
      for (const source of this._sources) {
        await this.manager.useSource(this.queryId, source);
      }
    }
    catch (error) {
      this._stopped = true;
      this._refreshInProgress = false;

      if (!this._isStream) {
        this.emit('error', error);
        this.emit('stop', error);
      }

      throw error;
    }

    let initialChanges;
    await this.client.lock.acquireAsync();
    try {
      // We create a temporary table into which we cache current results of the query.
      const results = await this.client.query({
        text: `
          START TRANSACTION;
          CREATE TEMPORARY TABLE "${this.queryId}_cache" AS EXECUTE "${this.queryId}_query";
          CREATE UNIQUE INDEX "${this.queryId}_cache_id" ON "${this.queryId}_cache" ("${this.options.uniqueColumn}");
          SELECT 1 AS __op__, ARRAY[]::TEXT[] AS __columns__, ${this.options.mode === 'columns' ? `"${this.options.uniqueColumn}"` : '*'} FROM "${this.queryId}_cache";
          COMMIT;
        `,
        types: this.types,
      });

      initialChanges = results[3].rows;
    }
    catch (error) {
      this._stopped = true;
      this._refreshInProgress = false;

      await this.client.query(`
        ROLLBACK;
      `);

      if (!this._isStream) {
        this.emit('error', error);
        this.emit('stop', error);
      }

      throw error;
    }
    finally {
      await this.client.lock.release();
    }

    if (!this._isStream) {
      this.emit('start');
    }

    this._processData(initialChanges);
    if (this._isStream) {
      this._streamPush({op: 'ready'});
    }
    else {
      this.emit('ready');
    }

    this._refreshInProgress = false;

    // If we successfully started, let us retry processing a source
    // changed event, if it happened while we were starting. If there was
    // an error things are probably going bad anyway so one skipped retry
    // should not be too problematic in such case.
    this._retrySourceChanged();
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

  // TODO: Should we try to cleanup as much as possible even when there are errors?
  //       Instead of stopping cleanup at the first error?
  async _stop(error) {
    if (this._stopped) {
      return;
    }

    this._stopped = true;

    if (!this._started) {
      return;
    }

    if (this._throttleTimeout) {
      clearTimeout(this._throttleTimeout);
      this._throttleTimeout = null;
    }

    try {
      // We drop triggers for each source not in a transaction, and especially not all of them
      // at once so that we do not have to lock all sources at the same time and potentially
      // introduce a deadlock if any of those sources is also used in some other materialized
      // view at the same time and it is just being refreshed.
      for (const source of this._sources) {
        await this.manager.releaseSource(this.queryId, source);
      }
    }
    catch (error) {
      if (!this._isStream) {
        this.emit('error', error);
        this.emit('stop', error);
      }

      throw error;
    }

    await this.client.lock.acquireAsync();
    try {
      await this.client.query(`
        DEALLOCATE "${this.queryId}_query";
        DROP TABLE "${this.queryId}_cache" CASCADE;
      `);
    }
    catch (error) {
      if (!this._isStream) {
        this.emit('error', error);
        this.emit('stop', error);
      }

      throw error;
    }
    finally {
      await this.client.lock.release();
    }

    if (!this._isStream) {
      this.emit('stop', error);
    }
  }

  _processData(changes) {
    for (const row of changes) {
      const op = OP_MAP.get(row.__op__);
      assert(op, `Unexpected query changed op '${row.__op__}'.`);
      delete row.__op__;
      const columns = row.__columns__;
      delete row.__columns__;
      if (op === 'update') {
        // TODO: Select and fetch only changed columns in "changed" mode.
        //       Currently we select all columns and remove unchanged columns on the client.
        if (this.options.mode === 'changed') {
          for (const key of Object.keys(row)) {
            if (key !== this.options.uniqueColumn && !columns.includes(key)) {
              delete row[key];
            }
          }
        }
        if (this._isStream) {
          this._streamPush({
            op,
            columns,
            row,
          });
        }
        else {
          this.emit(op, row, columns);
        }
      }
      else {
        if (this._isStream) {
          this._streamPush({
            op,
            row,
          });
        }
        else {
          this.emit(op, row);
        }
      }
    }

    if (this._isStream) {
      this._streamPush({op: 'refresh'});
    }
    else {
      this.emit('refresh');
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

    // Exit early and not try to lock.
    if (this._refreshInProgress) {
      return;
    }
    this._refreshInProgress = true;

    let changes;
    await this.client.lock.acquireAsync();
    try {
      // Create a new temporary table with new results of the query.
      // Computes a diff, swap the tables, and drop the old one.
      // TODO: Select and fetch only changed columns in "changed" mode.
      //       Currently we select all columns and remove unchanged columns on the client.
      const results = await this.client.query({
        text: `
          START TRANSACTION;
          CREATE TEMPORARY TABLE "${this.queryId}_new" AS EXECUTE "${this.queryId}_query";
          CREATE UNIQUE INDEX "${this.queryId}_new_id" ON "${this.queryId}_new" ("${this.options.uniqueColumn}");
          SELECT
            CASE WHEN "${this.queryId}_cache" IS NULL THEN 1
                 WHEN "${this.queryId}_new" IS NULL THEN 3
                 ELSE 2
            END AS __op__,
            CASE WHEN "${this.queryId}_cache" IS NULL THEN ARRAY[]::TEXT[]
                 WHEN "${this.queryId}_new" IS NULL THEN ARRAY[]::TEXT[]
                 ELSE (SELECT COALESCE(array_agg(row1.key), ARRAY[]::TEXT[]) FROM each(hstore("${this.queryId}_new")) AS row1 INNER JOIN each(hstore("${this.queryId}_cache")) AS row2 ON (row1.key=row2.key) WHERE row1.value IS DISTINCT FROM row2.value)
            END AS __columns__,
            (COALESCE("${this.queryId}_new", ROW("${this.queryId}_cache".*)::"${this.queryId}_new")).${this.options.mode === 'columns' ? `"${this.options.uniqueColumn}"` : '*'}
            FROM "${this.queryId}_cache" FULL OUTER JOIN "${this.queryId}_new" ON ("${this.queryId}_cache"."${this.options.uniqueColumn}"="${this.queryId}_new"."${this.options.uniqueColumn}")
            WHERE "${this.queryId}_cache" IS NULL OR "${this.queryId}_new" IS NULL OR "${this.queryId}_cache" OPERATOR(pg_catalog.*<>) "${this.queryId}_new";
          DROP TABLE "${this.queryId}_cache";
          ALTER TABLE "${this.queryId}_new" RENAME TO "${this.queryId}_cache";
          ALTER INDEX "${this.queryId}_new_id" RENAME TO "${this.queryId}_cache_id";
          COMMIT;
        `,
        types: this.types,
      });

      changes = results[3].rows;
    }
    catch (error) {
      this._refreshInProgress = false;

      await this.client.query(`
        ROLLBACK;
      `);

      throw error;
    }
    finally {
      await this.client.lock.release();
    }

    this._processData(changes);

    this._refreshInProgress = false;

    // If we successfully refreshed, let us retry processing a source
    // changed event, if it happened while we were refreshing. If there was
    // an error things are probably going bad anyway so one skipped retry
    // should not be too problematic in such case.
    this._retrySourceChanged();
  }

  _onSourceChanged() {
    if (!this._started || this._stopped) {
      return;
    }

    const timestamp = Date.now();
    if (!this._throttleTimestamp) {
      this._throttleTimestamp = timestamp;
    }

    // If stream is paused, if queue is not empty, or we are in refresh, we set
    // a flag to postpone processing this source changed event. We retry once
    // stream is not paused anymore or once we emptied the queue.
    if (this._streamPaused || this._streamQueue.length || this._refreshInProgress) {
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
      this._throttleTimeout = setTimeout(() => {
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
      this._retrySourceChanged();
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
    this._retrySourceChanged();
  }

  _retrySourceChanged() {
    if (this._sourceChangedPending) {
      this._sourceChangedPending = false;
      this._onSourceChanged();
    }
  }
}

const DEFAULT_MANAGER_OPTIONS = {
  maxConnections: 10,
  connectionConfig: {},
  handleClass: ReactiveQueryHandle,
};

const NOTIFICATION_REGEX = /^(.+)_(source_changed)$/;

// TODO: Disconnect idle clients after some time.
//       Idle meaning that they do not have any reactive queries using them.
class Manager extends EventEmitter {
  constructor(options={}) {
    super();

    this.options = Object.assign({}, DEFAULT_MANAGER_OPTIONS, options);

    if (!(this.options.maxConnections > 0)) {
      throw new Error("\"maxConnections\" option has to be larger than 0.")
    }

    this.managerId = null;

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
    // Map between a source name and a list of ids of reactive queries using it.
    this._sources = new Map();
  }

  async start() {
    if (this._started) {
      throw new Error("Manager has already been started.");
    }
    if (this._stopped) {
      throw new Error("Manager has already been stopped.");
    }

    this._started = true;

    this.managerId = await randomId();

    try {
      this.client = new Client(this.options.connectionConfig);
      this.client.lock = new AwaitLock();

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

      // We define the function as temporary for every client so that triggers using
      // it are dropped when the client disconnects (session ends).
      await this.client.query(`
        CREATE OR REPLACE FUNCTION pg_temp.notify_source_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
          DECLARE
            manager_id TEXT := TG_ARGV[0];
          BEGIN
            IF (TG_OP = 'INSERT') THEN
              PERFORM * FROM new_table LIMIT 1;
              IF FOUND THEN
                EXECUTE 'NOTIFY "' || manager_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
              END IF;
            ELSIF (TG_OP = 'UPDATE') THEN
              PERFORM * FROM ((TABLE new_table EXCEPT TABLE new_table) UNION ALL (TABLE new_table EXCEPT TABLE old_table)) AS differences LIMIT 1;
              IF FOUND THEN
                EXECUTE 'NOTIFY "' || manager_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
              END IF;
            ELSIF (TG_OP = 'DELETE') THEN
              PERFORM * FROM old_table LIMIT 1;
              IF FOUND THEN
                EXECUTE 'NOTIFY "' || manager_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
              END IF;
            ELSIF (TG_OP = 'TRUNCATE') THEN
              EXECUTE 'NOTIFY "' || manager_id || '_source_changed", ''{"name": "' || TG_TABLE_NAME || '", "schema": "' || TG_TABLE_SCHEMA || '"}''';
            END IF;
            RETURN NULL;
          END
        $$;
      `);

      // We use hstore to compute which columns changed.
      // TODO: It is possible that the user does not have permissions to create extensions.
      await this.client.query(`
        CREATE EXTENSION IF NOT EXISTS hstore;
      `);

      this.emit('connect', this.client);
    }
    catch (error) {
      this._stopped = true;

      this.emit('error', error);
      this.emit('stop', error);

      throw error;
    }

    this.emit('start');
  }

  // TODO: Should we try to cleanup as much as possible even when there are errors?
  //       Instead of stopping cleanup at the first error?
  async stop(error) {
    if (this._stopped) {
      return;
    }

    this._stopped = true;

    if (!this._started) {
      return;
    }

    try {
      while (this._handlesForQuery.size) {
        const promises = [];
        for (const [queryId, handle] of this._handlesForQuery.entries()) {
          // "stop" triggers "end" callback which removes the handle.
          promises.push(handle.stop());
        }
        await Promise.all(promises);
      }

      // All sources should be released when we called "stop" on all handles.
      assert(this._sources.size === 0, "\"sources\" should be empty.");

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
      }).catch((error) => {
        this.emit('error', error);
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
    const client = availableClients[Math.floor(Math.random() * availableClients.length)];

    // We mark it as used.
    this._useClient(client);

    // And return it.
    return client;
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

  async useSource(queryId, source) {
    if (!this._sources.has(source)) {
      this._sources.set(source, []);
    }
    this._sources.get(source).push(queryId);

    // The first time this source is being used.
    if (this._sources.get(source).length === 1) {
      await this.client.lock.acquireAsync();
      try {
        await this.client.query(`
          LISTEN "${this.managerId}_source_changed";
        `);

        try {
          await this.client.query(`
            START TRANSACTION;
            CREATE TRIGGER "${this.managerId}_source_changed_${source}_insert" AFTER INSERT ON "${source}" REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_source_changed('${this.managerId}');
            CREATE TRIGGER "${this.managerId}_source_changed_${source}_update" AFTER UPDATE ON "${source}" REFERENCING NEW TABLE AS new_table OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_source_changed('${this.managerId}');
            CREATE TRIGGER "${this.managerId}_source_changed_${source}_delete" AFTER DELETE ON "${source}" REFERENCING OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_source_changed('${this.managerId}');
            COMMIT;
          `);
        }
        catch (error) {
          await this.client.query(`
            ROLLBACK;
          `);

          throw error;
        }

        try {
          await this.client.query(`
            CREATE TRIGGER "${this.managerId}_source_changed_${source}_truncate" AFTER TRUNCATE ON "${source}" FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_source_changed('${this.managerId}');
          `);
        }
        // Ignoring errors. The source might not support TRUNCATE trigger.
        // For example, tables do, but views do not.
        catch (error) {}
      }
      catch (error) {
        this.emit('error', error);

        throw error;
      }
      finally {
        await this.client.lock.release();
      }
    }
  }

  async releaseSource(queryId, source) {
    const handles = this._sources.get(source) || [];
    const index = handles.indexOf(queryId);
    if (index >= 0) {
      handles.splice(index, 1);
    }
    else {
      console.warn(`Releasing a source '${source}' which is not being used by queryId '${queryId}'.`);
      return;
    }

    // Source is not used by anyone anymore.
    if (handles.length === 0) {
      this._sources.delete(source);

      await this.client.lock.acquireAsync();
      try {
        await this.client.query(`
          UNLISTEN "${this.managerId}_source_changed";
          DROP TRIGGER IF EXISTS "${this.managerId}_source_changed_${source}_insert" ON "${source}";
          DROP TRIGGER IF EXISTS "${this.managerId}_source_changed_${source}_update" ON "${source}";
          DROP TRIGGER IF EXISTS "${this.managerId}_source_changed_${source}_delete" ON "${source}";
          DROP TRIGGER IF EXISTS "${this.managerId}_source_changed_${source}_truncate" ON "${source}";
        `);
      }
      catch (error) {
        this.emit('error', error);

        throw error;
      }
      finally {
        await this.client.lock.release();
      }
    }
  }

  async _createClient() {
    const client = new Client(this.options.connectionConfig);
    client.lock = new AwaitLock();

    client.on('error', (error) => {
      this.emit('error', error, client);
    });
    client.on('end', () => {
      this.emit('disconnect', client);
    });

    await client.connect();

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
    try {
      const handle = new this.options.handleClass(this, client, queryId, query, options);
      this._setHandleForQuery(handle, queryId);
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
    catch (error) {
      // There was an error, client will not really be used.
      this._releaseClient(client);

      throw error;
    }
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

    const payload = JSON.parse(message.payload);

    const managerId = match[1];
    const notificationType = match[2];

    assert(managerId === this.managerId, "Notification id should match manager's id.");

    if (notificationType === 'source_changed') {
      // We ignore notifications for unknown sources.
      for (const queryId of (this._sources.get(payload.name) || [])) {
        const handle = this._getHandleForQuery(queryId);
        handle._onSourceChanged();
      }
    }
    else {
      console.warn(`Unknown notification type '${notificationType}'.`);
    }
  }
}

module.exports = {
  Manager,
  ReactiveQueryHandle,
};
