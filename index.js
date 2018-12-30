const EventEmitter = require('events');
const assert = require('assert');

const {Client} = require('pg');

const {randomId} = require('./random');

const DEFAULT_QUERY_OPTIONS = {
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

    const client = await this.manager.reserveClientForQuery(this.queryId);
    try {
      const {rows: queryExplanation} = await client.query({text: `EXPLAIN (FORMAT JSON) (${this.query})`, rowMode: 'array'});

      this._sources = [...this._extractSources(queryExplanation)].sort();

      // TODO: Handle also TRUNCATE of sources? What if it is not really a table but a view or something else?
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
      await this.manager.releaseClient(client);
    }
  }

  async stop() {
    if (this._stopped) {
      return;
    }

    this._stopped = true;

    const sourcesTriggers = this._sources.map((source) => {
      return `
        DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_insert" ON "${source}";
        DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_update" ON "${source}";
        DROP TRIGGER IF EXISTS "${this.queryId}_source_changed_${source}_delete" ON "${source}";
      `;
    }).join('\n');

    const client = await this.manager.reserveClientForQuery(this.queryId);
    try {
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
      await this.manager.releaseClient(client);
    }

    this.emit('end');
  }

  async _onQueryReady(payload) {
    this._readyPending = true;
  }

  async _onQueryRefreshed(payload) {
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
      await this.manager.releaseClient(client);
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
      await this.manager.releaseClient(client);
    }
  }

  async _onSourceChanged(payload) {
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

class Manager {
  constructor(options={}) {
    this.options = Object.assign({}, DEFAULT_MANAGER_OPTIONS, options);
    this._handles = new Map();
  }

  async start() {
    // TODO: Implement.
    this._client = await this._createClient();

    await this._client.query(`
      CREATE EXTENSION IF NOT EXISTS hstore;
    `);
  }

  async stop() {
    // TODO: Implement.
    while (this._handles.size) {
      for (const [key, value] of this._handles.entries()) {
        await value.stop();
        this._handles.delete(key);
      }
    }

    await this._client.end();
  }

  async reserveClient() {
    // TODO: Implement.
    return this._client;
  }

  async reserveClientForQuery(queryId) {
    // TODO: Implement.
    return this._client;
  }

  async releaseClient(client) {
    // TODO: Implement.
  }

  _setClientForQuery(client, queryId) {
    // TODO: Implement.
  }

  _deleteClientForQuery(queryId) {
    // TODO: Implement.
  }

  _setHandleForQuery(handle, queryId) {
    this._handles.set(queryId, handle);
  }

  _getHandleForQuery(queryId) {
    return this._handles.get(queryId);
  }

  _deleteHandleForQuery(queryId) {
    this._handles.delete(queryId);
  }

  async _createClient() {
    const client = new Client(this.options.connectionConfig);

    client.on('error', (error) => {
      // TODO: Redirect to all queries on this client and stop them.
      console.error("PostgreSQL client error.", error);
    });
    client.on('notice', (notice) => {
      console.warn("PostgreSQL notice.", notice.message, Object.assign({}, notice));
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

    return client;
  }

  async query(query, options) {
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
      await this.releaseClient(client);
    }
  }

  _onNotification(message) {
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
