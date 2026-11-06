import { Client, escapeIdentifier, escapeLiteral } from "pg";
import { databaseUrl } from "../src/config.ts";

const password = process.env.GRAFANA_DB_PASSWORD;
if (!password) throw new Error("Set GRAFANA_DB_PASSWORD in .env before creating the Grafana reader.");

const client = new Client({ connectionString: databaseUrl, connectionTimeoutMillis: 8_000, statement_timeout: 5_000 });
try {
  await client.connect();
  await client.query("BEGIN");
  // CREATE ROLE does not accept query parameters. Escape the password as a SQL literal.
  await client.query(
    "CREATE ROLE grafana_reader LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION PASSWORD " +
      escapeLiteral(password),
  );
  const result = await client.query<{ name: string }>("SELECT current_database() AS name");
  await client.query("GRANT CONNECT ON DATABASE " + escapeIdentifier(result.rows[0]!.name) + " TO grafana_reader");
  await client.query("GRANT USAGE ON SCHEMA public TO grafana_reader");
  await client.query("GRANT SELECT ON TABLE public.events TO grafana_reader");
  await client.query("ALTER ROLE grafana_reader SET statement_timeout = '5s'");
  await client.query("COMMIT");
  console.log("Created grafana_reader with SELECT access to public.events.");
} catch {
  await client.query("ROLLBACK").catch(() => {});
  console.error(
    "Grafana reader setup failed. Check database access, role-creation permission, and whether grafana_reader already exists.",
  );
  process.exitCode = 1;
} finally {
  await client.end();
}
