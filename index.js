const {Client} = require('pg');

const {randomId} = require('./random');

const DEFAULT_QUERY_OPTIONS = {
  primaryColumn: 'id',
};

const DEFAULT_MANAGER_OPTIONS = {
  maxConnections: 10,
  sourceDebounceWait: 100, // ms
  connectionConfig: {},
};

const NOTIFICATION_REGEX = /^(.+)_(query_changed|source_changed)$/;

class ReactiveQueryHandle {
  constructor(manager, queryId, options) {
    this.manager = manager;
    this.queryId = queryId;
    this.options = options;

    this.manager._handles.set(this.queryId, this);
  }

  async initialize(client, query) {
    const {rows: queryExplanation} = await client.query({text: `EXPLAIN (FORMAT JSON) (${query})`, rowMode: 'array'});

    const sources = [...this._extractSources(queryExplanation)].sort();

    client.query(`
      LISTEN "${this.queryId}_query_changed";
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
    // the materialized view. We then refresh materialized view for the first time.
    await client.query(`
      START TRANSACTION;
      CREATE TEMPORARY MATERIALIZED VIEW "${this.queryId}_view" AS ${query} WITH NO DATA;
      CREATE UNIQUE INDEX "${this.queryId}_view_id" ON "${this.queryId}_view" ("${this.options.primaryColumn}");
      ${sourcesTriggers}
      CREATE TRIGGER "${this.queryId}_query_changed_insert" AFTER INSERT ON "${this.queryId}_view" REFERENCING NEW TABLE AS new_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_query_changed('${this.queryId}', '${this.options.primaryColumn}');
      CREATE TRIGGER "${this.queryId}_query_changed_update" AFTER UPDATE ON "${this.queryId}_view" REFERENCING NEW TABLE AS new_table OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_query_changed('${this.queryId}', '${this.options.primaryColumn}');
      CREATE TRIGGER "${this.queryId}_query_changed_delete" AFTER DELETE ON "${this.queryId}_view" REFERENCING OLD TABLE AS old_table FOR EACH STATEMENT EXECUTE FUNCTION pg_temp.notify_query_changed('${this.queryId}', '${this.options.primaryColumn}');
      REFRESH MATERIALIZED VIEW CONCURRENTLY "${this.queryId}_view";
      COMMIT;
    `);
  }

  async end() {
    this.manager._handles.delete(this.queryId);
  }

  _onQueryChanged(payload) {
    // TODO: Implement.
    console.log("query changed", payload);
  }

  _onSourceChanged(payload) {
    // TODO: Implement.
    console.log("source changed", payload);
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

  async initialize() {
    this._client = await this._createClient();
  }

  async end() {

  }

  async reserveClient() {
    return this._client;
  }

  async releaseClient(client) {

  }

  async getClientForQuery(queryId) {
    return this._client;
  }

  _setQueryForClient(client, queryId) {

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

    let handle = null;
    const queryId = await randomId();
    const client = await this.reserveClient();
    try {
      handle = new ReactiveQueryHandle(this, queryId, options);
      await handle.initialize(client, query);
      this._setQueryForClient(client, queryId);
      return handle;
    }
    catch (error) {
      if (handle) {
        handle.end();
      }
      throw error;
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

    const handle = this._handles.get(queryId);
    if (!handle) {
      console.warn(`QueryId '${queryId}' without a handle.`);
      return;
    }

    const payload = JSON.parse(message.payload);

    if (notificationType === 'query_changed') {
      handle._onQueryChanged(payload);
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
