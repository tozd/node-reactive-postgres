const EventEmitter = require('events');

const {Client} = require('pg');

const {randomId} = require('./random');

const DEFAULT_QUERY_OPTIONS = {
  uniqueColumn: 'id',
  refreshDebounceWait: 100, // ms
  // Can be "id", "full", "changed", "idRow", "fullRow", "changedRow".
  mode: 'id',
  // When mode is "full", "changed", "fullRow", "changedRow", in how large batches do we fetch data inside one refresh?
  // 0 means only one query per refresh.
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

      const sources = [...this._extractSources(queryExplanation)].sort();

      client.query(`
        LISTEN "${this.queryId}_query_ready";
        LISTEN "${this.queryId}_query_changed";
        LISTEN "${this.queryId}_query_refreshed";
        LISTEN "${this.queryId}_source_changed";
      `);

      // TODO: Handle also TRUNCATE of sources? What if it is not really a table but a view or something else?
      const sourcesTriggers = sources.map((source) => {
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
    // TODO: Implement.
    this._stopped = true;
  }

  async _onQueryReady(payload) {
    this.emit('ready');
  }

  async _onQueryRefreshed(payload) {
    // TODO: Implement batching.
    if (this._dirty) {
      this.emit('refreshed');
      this._dirty = false;
    }
  }

  async _onQueryChanged(payload) {
    this._dirty = true;

    let row;
    if (this.options.mode === 'id') {
      row = {};
      row[this.options.uniqueColumn] = payload.id;
    }
    else if (this.options.mode === 'idRow') {
      row = [payload.id];
    }

    // TODO: Implement fetching and batching.
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
      console.error(`Unknown query changed op '${payload.op}'.`);
    }
  }

  async _onSourceChanged(payload) {
    // TODO: Implement debounce.
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
    // TODO: Send which columns have been updated in "notify_query_changes".
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
