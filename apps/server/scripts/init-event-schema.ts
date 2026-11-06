import { readFile } from 'node:fs/promises';
import { Pool } from 'pg';
import { databaseUrl } from '../src/config.ts';

const pool = new Pool({ connectionString: databaseUrl, connectionTimeoutMillis: 3_000 });
try {
  await pool.query(await readFile(new URL('../sql/001-events.sql', import.meta.url), 'utf8'));
  console.log('The event table and indexes are ready.');
} finally {
  await pool.end();
}
