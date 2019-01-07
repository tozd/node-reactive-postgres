// docker run -d --name postgres -e POSTGRES_PASSWORD=pass -p 5432:5432 postgres:11.1

const {Pool} = require('pg');
const through2 = require('through2');
const {UNMISTAKABLE_CHARS} = require('@tozd/random-id');

const {Manager} = require('./index');

const CONNECTION_CONFIG = {
  user: 'postgres',
  database: 'postgres',
  password: 'pass',
};

const jsonStream = through2.obj(function (chunk, encoding, callback) {
  this.push(JSON.stringify(chunk, null, 2) + '\n');
  callback();
});

const pool = new Pool(CONNECTION_CONFIG);

const manager = new Manager({
  connectionConfig: CONNECTION_CONFIG,
});

manager.on('start', () => {
  console.log(new Date(), 'manager start');
});

manager.on('error', (error, client) => {
  console.log(new Date(), 'manager error', error);
});

manager.on('stop', (error) => {
  console.log(new Date(), 'manager stop', error);
});

manager.on('connect', (client) => {
  client.on('notice', (notice) => {
    console.warn(new Date(), notice.message, Object.assign({}, notice));
  });
});

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

(async () => {
  await manager.start();

  await pool.query(`
    CREATE OR REPLACE FUNCTION random_id() RETURNS TEXT LANGUAGE SQL AS $$
      SELECT array_to_string(
        array(
          SELECT SUBSTRING('${UNMISTAKABLE_CHARS}' FROM floor(random()*55)::int+1 FOR 1) FROM generate_series(1, 17)
        ),
        ''
      );
    $$;
    DROP TABLE IF EXISTS comments CASCADE;
    DROP TABLE IF EXISTS posts CASCADE;
    CREATE TABLE posts (
      "_id" CHAR(17) PRIMARY KEY DEFAULT random_id(),
      "body" JSONB NOT NULL DEFAULT '{}'::JSONB
    );
    CREATE TABLE comments (
      "_id" CHAR(17) PRIMARY KEY DEFAULT random_id(),
      "postId" CHAR(17) NOT NULL REFERENCES posts("_id"),
      "body" JSONB NOT NULL DEFAULT '{}'::JSONB
    );
  `);

  let result;
  for (let i = 0; i < 5; i++) {
    result = await pool.query(`
      INSERT INTO posts ("body") VALUES($1) RETURNING _id;
    `, [{'title': `Post title ${i}`}]);

    const postId = result.rows[0]._id;

    for (let j = 0; j < 10; j++) {
      await pool.query(`
        INSERT INTO comments ("postId", "body") VALUES($1, $2);
      `, [postId, {'title': `Comment title ${j}`}]);
    }
  }

  const queries = [
    // All comments with embedded post.
    `SELECT "_id", "body", (SELECT to_jsonb(posts) FROM posts WHERE posts."_id"=comments."postId") AS "post" FROM comments`,
    // All posts with embedded comments.
    `SELECT "_id", "body", (SELECT to_jsonb(COALESCE(array_agg(comments), '{}')) FROM comments WHERE comments."postId"=posts."_id") AS "comments" FROM posts`,
  ];

  let handle1 = await manager.query(queries[0], {uniqueColumn: '_id', mode: 'changed'});

  handle1.on('start', () => {
    console.log(new Date(), 'query start', handle1.queryId);
  });

  handle1.on('ready', () => {
    console.log(new Date(), 'query ready', handle1.queryId);
  });

  handle1.on('refresh', () => {
    console.log(new Date(), 'query refresh', handle1.queryId);
  });

  handle1.on('insert', (row) => {
    console.log(new Date(), 'insert', handle1.queryId, row);
  });

  handle1.on('update', (row, columns) => {
    console.log(new Date(), 'update', handle1.queryId, row, columns);
  });

  handle1.on('delete', (row) => {
    console.log(new Date(), 'delete', handle1.queryId, row);
  });

  handle1.on('error', (error) => {
    console.log(new Date(), 'query error', handle1.queryId, error);
  });

  handle1.on('stop', (error) => {
    console.log(new Date(), 'query stop', handle1.queryId, error);
  });

  await handle1.start();

  const handle2 = await manager.query(queries[1], {uniqueColumn: '_id', mode: 'changed'});

  handle2.on('close', () => {
    console.log(new Date(), 'query pipe close', handle2.queryId);
  });

  handle2.on('error', (error) => {
    console.log(new Date(), 'query pipe error', handle2.queryId, error);
  });

  handle2.pipe(jsonStream).pipe(process.stdout);

  await sleep(1000);

  let commentIds = [];
  for (let i = 5; i < 7; i++) {
    result = await pool.query(`
      INSERT INTO posts ("body") VALUES($1) RETURNING _id;
    `, [{'title': `Post title ${i}`}]);

    const postId = result.rows[0]._id;

    for (let j = 0; j < 10; j++) {
      result = await pool.query(`
        INSERT INTO comments ("postId", "body") VALUES($1, $2) RETURNING _id;
      `, [postId, {'title': `Comment title ${j}`}]);

      commentIds.push(result.rows[0]._id);
    }
  }

  await sleep(1000);

  for (let i = 0; i < commentIds.length; i++) {
    await pool.query(`
      UPDATE comments SET "body"=$1 WHERE "_id"=$2;
    `, [{'title': `Comment new title ${i}`}, commentIds[i]]);
  }

  await sleep(1000);

  await pool.query(`
    DELETE FROM comments WHERE "_id"=ANY($1);
  `, [commentIds]);

  await sleep(1000);

  await pool.end();
  await manager.stop();
})();
