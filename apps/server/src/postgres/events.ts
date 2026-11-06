import type { Pool } from "pg";
import { eventError } from "../../../../shared/log-event.ts";
import type { LogEvent } from "../../../../shared/log-event.ts";

export async function insertEvent(pool: Pool, value: unknown): Promise<void> {
  if (eventError(value) || (value as LogEvent).app !== "pin-demo") throw new Error("Kafka event failed validation.");
  const event = value as LogEvent;
  await pool.query(
    `INSERT INTO events (app, event_id, schema_version, event_time, app_version, environment, platform, event_name, severity, attributes)
     VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10::jsonb)
     ON CONFLICT (app, event_id) DO NOTHING`,
    [event.app, event.event_id, event.schema_version, event.event_time, event.app_version, event.environment,
      event.platform, event.event_name, event.severity, JSON.stringify(event.attributes)],
  );
}
