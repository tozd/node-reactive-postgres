const EventEmitter = require('events');

const {Client} = require('pg');

const {randomId} = require('./random');

const DEFAULT_QUERY_OPTIONS = {
  uniqueColumn: 'id',
  refreshDebounceWait: 100, // ms
  // Can be "id", "columns", "fullObject", "changedObject", "fullRow", "changedRow".
  mode: 'id',
  // When mode is "fullObject", "changedObject", "fullRow", "changedRow", do we batch fetching of data or not?
  batchByRefresh: true,
  // Custom type parsers.
  types: null,
};

const DEFAULT_MANAGER_OPTIONS = {
  maxConnections: 10,
  connectionConfig: {},
};

const NOTIFICATION_REGEX = /^(.+)_(query_ready|query_changed|query_refreshed|source_changed)$/;

class ReactiveQueryHandle extends EventEmitter {
  constructor(manager, queryId, query, options) {
    super();

    this.manager = manager;
    this.queryId = queryId;
    this.query = query;
    this.options = options;

    this._started = false;
    this._stopped = false;
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
    this.emit('refreshed');
  }

  async _onQueryChanged(payload) {
    // TODO: Implement.
    console.log(new Date(), "query changed", payload);
  }

  async _onSourceChanged(payload) {
    // TODO: Implement debounce.
    console.log(new Date(), "source changed", payload);
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
    // TODO: Send which columns have been updated in "notify_query_changes".
    await client.query(`
      CREATE OR REPLACE FUNCTION pg_temp.notify_query_changed() RETURNS TRIGGER LANGUAGE plpgsql AS $$
        DECLARE
          query_id TEXT := TG_ARGV[0];
          primary_column TEXT := TG_ARGV[1];
        BEGIN
          IF (TG_OP = 'INSERT') THEN
            EXECUTE 'SELECT pg_notify(''' || query_id || '_query_changed'', row_to_json(changes)::text) FROM (SELECT ''insert'' AS op, "' || primary_column || '" AS id FROM new_table) AS changes';
          ELSIF (TG_OP = 'UPDATE') THEN
            EXECUTE 'SELECT pg_notify(''' || query_id || '_query_changed'', row_to_json(changes)::text) FROM (SELECT ''update'' AS op, "' || primary_column || '" AS id FROM new_table) AS changes';
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
