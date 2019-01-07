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
