import { Pool } from "pg";
import { Kafka } from "kafkajs";
import { buildServer } from "./app.ts";
import { databaseUrl, kafkaBrokers, kafkaDeadLetterTopic, kafkaTopic } from "./config.ts";

const port = Number(process.env.PORT ?? 3001);

if (!Number.isInteger(port) || port < 1 || port > 65535) throw new Error("PORT must be an integer from 1 to 65535.");

const pool = new Pool({
  connectionString: databaseUrl,
  max: 5,
  connectionTimeoutMillis: 3_000,
  statement_timeout: 5_000,
});

const kafka = new Kafka({
  clientId: "client-logging-api",
  brokers: kafkaBrokers,
  retry: { retries: 2 },
});

const producer = kafka.producer();
const admin = kafka.admin();
const app = buildServer(pool, producer, admin);
let closing = false;
let retry: NodeJS.Timeout | undefined;
let connecting: Promise<void> | undefined;

pool.on("error", (error) => app.log.error({ err: error }, "Idle database connection failed"));

const retention = setInterval(
  () => {
    pool
      .query("DELETE FROM events WHERE app = 'pin-demo' AND received_at < now() - interval '7 days'")
      .catch((error) => app.log.error({ err: error }, "Retention cleanup failed"));
  },
  60 * 60 * 1000,
);
retention.unref();

app.addHook("onClose", async () => {
  closing = true;
  clearTimeout(retry);
  clearInterval(retention);
  await connecting;
  await Promise.all([producer.disconnect(), admin.disconnect(), pool.end()]);
});

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void app.close();
  });
}

async function connectKafka() {
  try {
    await admin.connect();
    await admin.createTopics({
      topics: [
        { topic: kafkaTopic, numPartitions: 1, replicationFactor: 1 },
        {
          topic: kafkaDeadLetterTopic,
          numPartitions: 1,
          replicationFactor: 1,
          configEntries: [{ name: "retention.ms", value: String(7 * 24 * 60 * 60 * 1000) }],
        },
      ],
      waitForLeaders: true,
    });
    await producer.connect();
    app.log.info("Kafka connected");
  } catch (error) {
    app.log.warn({ err: error }, "Kafka unavailable; retrying in 5 seconds");
    if (!closing)
      retry = setTimeout(() => {
        connecting = connectKafka();
      }, 5_000);
  }
}

try {
  await app.listen({ host: "127.0.0.1", port });
  connecting = connectKafka();
} catch (error) {
  app.log.error(error);
  await app.close();
  process.exitCode = 1;
}
