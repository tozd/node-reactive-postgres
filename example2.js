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
    uniqueColumn: '_id',
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
