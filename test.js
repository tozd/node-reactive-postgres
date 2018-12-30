// docker run -d --name postgres -e POSTGRES_PASSWORD=pass -p 5432:5432 mitar/postgres:latest

const {Pool} = require('pg');

const {Manager} = require('./index');
const {UNMISTAKABLE_CHARS} = require('./random');

const CONNECTION_CONFIG = {
  user: 'postgres',
  database: 'postgres',
  password: 'pass',
};

const pool = new Pool(CONNECTION_CONFIG);

const manager = new Manager({
  connectionConfig: CONNECTION_CONFIG,
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
    CREATE TABLE IF NOT EXISTS posts (
      "_id" CHAR(17) PRIMARY KEY DEFAULT random_id(),
      "body" JSONB NOT NULL DEFAULT '{}'::JSONB
    );
    CREATE TABLE IF NOT EXISTS comments (
      "_id" CHAR(17) PRIMARY KEY DEFAULT random_id(),
      "postId" CHAR(17) NOT NULL REFERENCES Posts("_id"),
      "body" JSONB NOT NULL DEFAULT '{}'::JSONB
    );
    DELETE FROM comments;
    DELETE FROM posts;
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
    `SELECT "_id", "body", (SELECT row_to_json(posts) FROM posts WHERE posts."_id"=comments."postId") AS "post" FROM comments`,
    // All posts with embedded comments.
    `SELECT "_id", "body", (SELECT array_to_json(COALESCE(array_agg(row_to_json(comments)), ARRAY[]::JSON[])) FROM comments WHERE comments."postId"=posts."_id") AS "comments" FROM posts`,
  ];

  for (const query of queries) {
    const handle = await manager.query(query, {uniqueColumn: '_id', mode: 'changed'});

    handle.on('ready', () => {
      console.log(new Date(), 'query ready', handle.queryId);
    });

    handle.on('refreshed', () => {
      console.log(new Date(), 'query refreshed', handle.queryId);
    });

    handle.on('insert', (row) => {
      console.log(new Date(), 'insert', handle.queryId, row);
    });

    handle.on('update', (row, columns) => {
      console.log(new Date(), 'update', handle.queryId, row, columns);
    });

    handle.on('delete', (row) => {
      console.log(new Date(), 'delete', handle.queryId, row);
    });

    handle.on('error', (error) => {
      console.log(new Date(), 'query error', handle.queryId, error);
    });

    handle.on('end', (error) => {
      console.log(new Date(), 'query end', handle.queryId, error);
    });

    await handle.start();
  }

  await sleep(10 * 1000);

  await pool.end();
  await manager.stop();
})();
