const EventEmitter = require('events');
const assert = require('assert');

const {Client} = require('pg');

const {randomId} = require('./random');

const DEFAULT_QUERY_OPTIONS = {
  // TODO: Allow multi-column unique index as well.
  uniqueColumn: 'id',
  refreshDebounceWait: 100, // ms
  // Can be "id", "changed", "full".
  mode: 'id',
  // When mode is "changed" or "full", in how large batches do we fetch data inside one refresh?
  // 0 means only one batch per refresh.
  batchSize: 0,
  // Custom type parsers.
  types: null,
};

const DEFAULT_MANAGER_OPTIONS = {
  maxConnections: 10,
  connectionConfig: {},
};

const NOTIFICATION_REGEX = /^(.+)_(query_ready|query_changed|query_refreshed|source_changed)$/;

// TODO: Implement stream as well.
class ReactiveQueryHandle extends EventEmitter {
  constructor(manager, queryId, query, options) {
    super();

    this.manager = manager;
    this.queryId = queryId;
    this.query = query;
    this.options = options;

    this._started = false;
    this._stopped = false;
    this._dirty = false;
    this._batch = [];
    this._readyPending = false;
    this._sources = null;
  }

  async start() {
    if (this._started) {
      throw new Error("Query has already been started.");
    }
    if (this._stopped) {
      throw new Error("Query has already been stopped.");
    }

    this._started = true;

    let client = null;
    try {
      client = await this.manager.reserveClientForQuery(this.queryId);

      const {rows: queryExplanation} = await client.query({text: `EXPLAIN (FORMAT JSON) (${this.query})`, rowMode: 'array'});

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

      // We create a temporary materialized view based on a query, but without data, just
      // structure. We add triggers to notify the client about changes to sources and
      // the materialized view. We then refresh the materialized view for the first time.
      await client.query(`
        START TRANSACTION;
        LISTEN "${this.queryId}_query_ready";
        LISTEN "${this.queryId}_query_changed";
        LISTEN "${this.queryId}_query_refreshed";
        LISTEN "${this.queryId}_source_changed";
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
      this.emit('error', error);
      this.emit('end', error);
      throw error;
    }
    finally {
      if (client) {
        this.manager.releaseClient(client);
      }
    }

    this.emit('start');
  }

  async stop() {
    if (this._stopped) {
      return;
    }
    if (!this._started) {
      throw new Error("Query has not been started.");
    }

    this._stopped = true;

    let client = null;
    try {
      const sourcesTriggers = this._sources.map((source) => {
        return `
          DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_insert" ON "${source}";
          DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_update" ON "${source}";
          DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_delete" ON "${source}";
        `;
      }).join('\n');

      client = await this.manager.reserveClientForQuery(this.queryId);

      await client.query(`
        START TRANSACTION;
        UNLISTEN "${this.queryId}_query_ready";
        UNLISTEN "${this.queryId}_query_changed";
        UNLISTEN "${this.queryId}_query_refreshed";
        UNLISTEN "${this.queryId}_source_changed";
        ${sourcesTriggers}
        DROP MATERIALIZED VIEW IF EXISTS "${this.queryId}_view" CASCADE;
        COMMIT;
      `);
    }
    catch (error) {
      this.emit('error', error);
      this.emit('end', error);
      throw error;
    }
    finally {
      if (client) {
        this.manager.releaseClient(client);
      }
    }

    this.emit('end');
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
      this.emit('refreshed');
      if (this._readyPending) {
        this._readyPending = false;
        this.emit('ready');
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

      if (payload.op === 'insert') {
        this.emit('insert', row);
      }
      else if (payload.op === 'update') {
        this.emit('update', row, payload.columns);
      }
      else if (payload.op === 'delete') {
        this.emit('delete', row);
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
    const client = await this.manager.reserveClientForQuery(this.queryId);
    try {
      if (this.options.mode === 'changed') {
        const updateColumns = '"' + [...columnsToFetchForUpdate].join('", "') + '"';

        // We do one query for inserted rows.
        let response = await client.query({
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
        response = await client.query({
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
        const response = await client.query({
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
    }
    finally {
      this.manager.releaseClient(client);
    }

    for (const payload of batch) {
      if (payload.op === 'insert') {
        const row = rows.get(payload.id);
        // It could happen that row was deleted in meantime and we could not fetch it.
        // We do not do anything for now and leave it to a future notification to handle this.
        if (row) {
          this.emit('insert', row);
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
          this.emit('update', row, payload.columns);
        }
      }
      else if (payload.op === 'delete') {
        const row = {};
        row[this.options.uniqueColumn] = payload.id;
        this.emit('delete', row);
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

    const client = await this.manager.reserveClientForQuery(this.queryId);
    try {
      client.query(`
        START TRANSACTION;
        REFRESH MATERIALIZED VIEW CONCURRENTLY "${this.queryId}_view";
        NOTIFY "${this.queryId}_query_refreshed", '{}';
        COMMIT;
      `);
    }
    finally {
      this.manager.releaseClient(client);
    }
  }

  async _onSourceChanged(payload) {
    if (!this._started || this._stopped) {
      return;
    }

    // TODO: Implement debounce.
    await this.refresh();
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
}

// TODO: Disconnect idle clients after some time.
//       Idle meaning that they do not have any reactive query active.
class Manager extends EventEmitter {
  constructor(options={}) {
    super();

    this.options = Object.assign({}, DEFAULT_MANAGER_OPTIONS, options);

    if (!(this.options.maxConnections > 0)) {
      throw new Error("\"maxConnections\" option has to be larger than 0.")
    }

    this._started = false;
    this._stopped = false;
    this._stoppingInProgress = false;
    this._handlesForQuery = new Map();
    this._clientsForQuery = new Map();
    this._clientsUtilization = new Map();
    this._clients = new Map();
    this._pendingClients = [];
    this._reservedClients = new Set();
  }

  async start() {
    if (this._started) {
      throw new Error("Manager has already been started.");
    }
    if (this._stopped) {
      throw new Error("Manager has already been stopped.");
    }

    this._started = true;

    let client = null;
    try {
      client = await this.reserveClient();

      await client.query(`
        CREATE EXTENSION IF NOT EXISTS hstore;
      `);
    }
    catch (error) {
      this._stopped = true;
      this.emit('error', error);
      this.emit('end', error);
      throw error;
    }
    finally {
      if (client) {
        this.releaseClient(client);
      }
    }

    this.emit('start');
  }

  async stop() {
    if (this._stopped) {
      return;
    }
    if (!this._started) {
      throw new Error("Manager has not been started.");
    }

    this._stopped = true;
    this._stoppingInProgress = true;

    try {
      while (this._handlesForQuery.size) {
        for (const [queryId, handle] of this._handlesForQuery.entries()) {
          // "stop" triggers "end" callback which removes the handle.
          await handle.stop();
        }
      }

      this._stoppingInProgress = false;

      // They should all be removed now through "end" callbacks when we
      // called "stop" on all handles.
      assert(this._handlesForQuery.size === 0, "\"handlesForQuery\" should be empty.");
      assert(this._clientsForQuery.size === 0, "\"clientsForQuery\" should be empty.");

      // Disconnect all clients.
      while (this._pendingClients.size) {
        for (const [clientPromise, queue] of this._pendingClients.entries()) {
          while (queue.length) {
            const q = queue.shift();
            q(new Error("Manager stopping."));
          }

          const client = await clientPromise;
          await client.end();
          this._pendingClients.delete(clientPromise);
        }
      }
      while (this._clients.size) {
        for (const [client, queue] of this._clients.entries()) {
          while (queue.length) {
            const q = queue.shift();
            q(new Error("Manager stopping."));
          }

          await client.end();
          this._clients.delete(client);
        }
      }
    }
    catch (error) {
      this.emit('error', error);
      this.emit('end', error);
      throw error;
    }

    this.emit('end');
  }

  async reserveClient() {
    // We allow "stoppingInProgress" exception so that queries can cleaned up properly.
    if (this._stopped && !this._stoppingInProgress) {
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
        this._clients.set(client, []);
        const index = this._pendingClients.indexOf(clientPromise);
        if (index >= 0) {
          this._pendingClients.splice(index, 1);
        }
      });
      this._pendingClients.push(clientPromise);
    }

    await Promise.all(this._pendingClients);

    assert(this._clients.size > 0, "\"maxConnections\" has to be larger than 0.");

    // Find clients with the least number of active queries. We want primarily
    // to distribute all queries between all clients equally.
    let reasonableClients = [];
    let lowestUtilization = Number.POSITIVE_INFINITY;
    for (const client of this._clients.keys()) {
        const utilization = this._clientsUtilization.get(client) || 0;
        if (utilization < lowestUtilization) {
          lowestUtilization = utilization;
          reasonableClients = [client];
        }
        else if (utilization === lowestUtilization) {
          reasonableClients.push(client);
        }
    }

    const notReservedClients = [];
    for (const client of reasonableClients) {
      if (!this._reservedClients.has(client)) {
        notReservedClients.push(client);
      }
    }

    // We have clients which are not reserved. Let's pick one among them.
    if (notReservedClients.length) {
      const client = notReservedClients[Math.floor(Math.random() * notReservedClients.length)];
      this._reservedClients.add(client);
      return client;
    }

    let leastUsedClient = null;
    let leastUsedQueue = null;
    let leastUsedQueueLength = Number.POSITIVE_INFINITY;
    // Find a client with the shortest queue.
    for (const client of reasonableClients) {
      const queue = this._clients.get(client);
      if (queue.length < leastUsedQueueLength) {
        leastUsedQueueLength = queue.length;
        leastUsedClient = client;
        leastUsedQueue = queue;
      }
    }

    return new Promise((resolve, reject) => {
      leastUsedQueue.push((error) => {
        if (error) {
          reject(error);
        }
        // We allow "stoppingInProgress" exception so that queries can cleaned up properly.
        else if (this._stopped && !this._stoppingInProgress) {
          reject(new Error("Manager stopping."));
        }
        else {
          assert(!this._reservedClients.has(leastUsedClient), "Client is already reserved.");
          this._reservedClients.add(leastUsedClient);
          resolve(leastUsedClient);
        }
      });
    });
  }

  async reserveClientForQuery(queryId) {
    // We allow "stoppingInProgress" exception so that queries can cleaned up properly.
    if (this._stopped && !this._stoppingInProgress) {
      // This method is returning a client, so we throw.
      throw new Error("Manager has been stopped.");
    }
    if (!this._started) {
      throw new Error("Manager has not been started.");
    }

    const client = this._clientsForQuery.get(queryId);

    if (!this._reservedClients.has(client)) {
      this._reservedClients.add(client);
      return client;
    }

    const queue = this._clients.get(client);

    return new Promise((resolve, reject) => {
      queue.push((error) => {
        if (error) {
          reject(error);
        }
        // We allow "stoppingInProgress" exception so that queries can cleaned up properly.
        else if (this._stopped && !this._stoppingInProgress) {
          reject(new Error("Manager stopping."));
        }
        else {
          assert(!this._reservedClients.has(client), "Client is already reserved.");
          this._reservedClients.add(client);
          resolve(client);
        }
      });
    });
  }

  releaseClient(client) {
    // We allow "stoppingInProgress" exception so that queries can cleaned up properly.
    if (this._stopped && !this._stoppingInProgress) {
      // This method is not returning anything, so we just ignore the call.
      return;
    }
    if (!this._started) {
      throw new Error("Manager has not been started.");
    }

    this._reservedClients.delete(client);

    // We provide the client to the next reservation.
    const queue = this._clients.get(client);
    const q = queue.shift();
    if (q) {
      q();
    }
  }

  _setClientForQuery(client, queryId) {
    this._clientsForQuery.set(queryId, client);
    const utilization = this._clientsUtilization.get(client) || 0;
    this._clientsUtilization.set(client, utilization + 1);
  }

  _deleteClientForQuery(queryId) {
    const client = this._clientsForQuery.get(queryId);
    if (client) {
      this._clientsForQuery.delete(queryId);
      const utilization = this._clientsUtilization.get(client);
      if (utilization > 1) {
        this._clientsUtilization.set(client, utilization - 1);
      }
      else {
        this._clientsUtilization.delete(client);
      }
    }
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

  async _createClient() {
    const client = new Client(this.options.connectionConfig);

    client.on('error', (error) => {
      this.emit('error', error, client);
    });
    client.on('end', () => {
      this.emit('disconnect', client);
    });
    client.on('notification', (message) => {
      this._onNotification(message);
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

  async query(query, options) {
    if (this._stopped) {
      // This method is returning a handle, so we throw.
      throw new Error("Manager has been stopped.");
    }
    if (!this._started) {
      throw new Error("Manager has not been started.");
    }

    options = Object.assign({}, DEFAULT_QUERY_OPTIONS, options);

    const queryId = await randomId();
    const client = await this.reserveClient();
    try {
      const handle = new ReactiveQueryHandle(this, queryId, query, options);
      this._setClientForQuery(client, queryId);
      this._setHandleForQuery(handle, queryId);
      handle.once('end', (error) => {
        this._deleteClientForQuery(queryId);
        this._deleteHandleForQuery(queryId);
      });
      return handle;
    }
    finally {
      this.releaseClient(client);
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

    const queryId = match[1];
    const notificationType = match[2];

    const handle = this._getHandleForQuery(queryId);
    if (!handle) {
      console.warn(`QueryId '${queryId}' without a handle.`);
      return;
    }

    const payload = JSON.parse(message.payload);

    if (notificationType === 'query_ready') {
      handle._onQueryReady(payload);
    }
    else if (notificationType === 'query_changed') {
      handle._onQueryChanged(payload);
    }
    else if (notificationType === 'query_refreshed') {
      handle._onQueryRefreshed(payload);
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
};
