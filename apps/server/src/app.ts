import Fastify from "fastify";
import type { Pool } from "pg";
import type { Admin, Producer } from "kafkajs";
import { batchError, EVENT_LIMITS } from "../../../shared/log-event.ts";
import type { LogEvent } from "../../../shared/log-event.ts";
import { kafkaTopic } from "./config.ts";

export function buildServer(pool: Pool, producer: Producer, admin: Admin) {
  const app = Fastify({ logger: true, bodyLimit: EVENT_LIMITS.batchBytes, requestTimeout: 10_000 });

  app.addHook("onRequest", async (request, reply) => {
    const origin = request.headers.origin;

    if (
      origin &&
      !["http://127.0.0.1:5173", "http://localhost:5173", "http://127.0.0.1:4173", "http://localhost:4173"].includes(
        origin,
      )
    ) {
      return reply.code(403).send({ error: "This learning API accepts local demo origins only." });
    }
    if (request.headers["content-encoding"])
      return reply.code(415).send({ error: "Compressed requests are not supported." });
  });

  app.get("/health", async (_request, reply) => {
    try {
      await admin.fetchTopicMetadata({ topics: [kafkaTopic] });
      return { status: "ready", broker: "kafka" };
    } catch (error) {
      app.log.error({ err: error }, "Kafka readiness check failed");
      return reply.code(503).send({ status: "unavailable", broker: "kafka" });
    }
  });

  app.post("/v1/events", async (request, reply) => {
    const invalid = batchError(request.body);

    if (invalid) return reply.code(400).send({ error: invalid });

    const { events } = request.body as { events: LogEvent[] };

    if (events.some((event) => event.app !== "pin-demo")) {
      return reply.code(403).send({ error: "This local intake accepts only the pin-demo app." });
    }

    try {
      await producer.send({
        topic: kafkaTopic,
        messages: events.map((event) => ({ key: `${event.app}:${event.event_id}`, value: JSON.stringify(event) })),
        acks: -1,
        timeout: 8_000,
      });
      return { queued: events.length };
    } catch (error) {
      app.log.error({ err: error }, "Kafka publish failed");
      return reply
        .code(503)
        .send({ error: "Kafka could not confirm the batch. Keep event IDs unchanged if retrying." });
    }
  });

  app.get("/v1/events", async (request, reply) => {
    const query = request.query as Record<string, unknown>;
    const fields = ["run_id", "app_version", "event_name", "severity", "since", "until"] as const;
    if (
      Object.keys(query).some((key) => !fields.includes(key as (typeof fields)[number])) ||
      fields.some((key) => query[key] !== undefined && (typeof query[key] !== "string" || !query[key]))
    ) {
      return reply.code(400).send({ error: "Use one nonempty value for each supported filter." });
    }
    const { run_id, app_version, event_name, severity, since, until } = query as Record<
      (typeof fields)[number],
      string | undefined
    >;

    if (
      (run_id && !/^[a-zA-Z0-9_-]{1,64}$/.test(run_id)) ||
      (app_version && !/^[a-zA-Z0-9][a-zA-Z0-9.+_-]{0,63}$/.test(app_version)) ||
      (event_name && !/^[a-z][a-z0-9_.-]{0,95}$/.test(event_name)) ||
      (severity && !["info", "warn", "error"].includes(severity)) ||
      [since, until].some(
        (value) =>
          value &&
          (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.\d{3})?Z$/.test(value) ||
            !Number.isFinite(Date.parse(value)) ||
            ![new Date(value).toISOString(), new Date(value).toISOString().replace(".000Z", "Z")].includes(value)),
      ) ||
      (since && until && since > until)
    ) {
      return reply.code(400).send({ error: "Invalid event filter." });
    }

    const values = [
      run_id ?? null,
      app_version ?? null,
      event_name ?? null,
      severity ?? null,
      since ?? null,
      until ?? null,
    ];

    const where = `app = 'pin-demo'
      AND ($1::text IS NULL OR attributes->>'run_id' = $1)
      AND ($2::text IS NULL OR app_version = $2)
      AND ($3::text IS NULL OR event_name = $3)
      AND ($4::text IS NULL OR severity = $4)
      AND ($5::timestamptz IS NULL OR received_at >= $5)
      AND ($6::timestamptz IS NULL OR received_at <= $6)`;

    try {
      const [events, counts] = await Promise.all([
        pool.query(`SELECT * FROM events WHERE ${where} ORDER BY received_at DESC, event_id LIMIT 100`, values),

        pool.query(
          `SELECT app_version, count(*)::int AS total,
          count(*) FILTER (WHERE event_name = 'pin_save_failed')::int AS failures
          FROM events WHERE ${where} GROUP BY app_version ORDER BY app_version`,
          values,
        ),
      ]);

      return { events: events.rows, summary: counts.rows };
    } catch (error) {
      app.log.error({ err: error }, "Event query failed");
      return reply.code(503).send({ error: "Storage is unavailable." });
    }
  });

  return app;
}
