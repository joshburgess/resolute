import pg from 'pg';

const url = process.env.PG_URL;
if (!url) {
  console.error('PG_URL is required');
  process.exit(1);
}

const client = new pg.Client({ connectionString: url });
await client.connect();

await client.query(`
  DROP TABLE IF EXISTS bench_rows;
  CREATE TABLE bench_rows (
    id bigint PRIMARY KEY,
    name text NOT NULL,
    score int NOT NULL,
    created_at timestamptz NOT NULL DEFAULT now()
  );
`);

const rows = 10_000;
const values: string[] = [];
const params: Array<string | number> = [];
for (let i = 1; i <= rows; i++) {
  values.push(`($${params.length + 1}, $${params.length + 2}, $${params.length + 3})`);
  params.push(i, `name-${i}`, (i * 2654435761) & 0x7fffffff);
}
await client.query(
  `INSERT INTO bench_rows (id, name, score) VALUES ${values.join(',')}`,
  params,
);

console.log(`Seeded ${rows} rows into bench_rows.`);
await client.end();
