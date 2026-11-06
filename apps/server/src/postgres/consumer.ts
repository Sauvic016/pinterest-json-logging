import { Kafka } from "kafkajs";
import { Pool } from "pg";
import { databaseUrl, kafkaBrokers, kafkaTopic } from "../config.ts";
import { processPostgresRecord } from "./process-record.ts";

const pool = new Pool({
  connectionString: databaseUrl,
  max: 5,
  connectionTimeoutMillis: 3_000,
  statement_timeout: 5_000,
});

const kafka = new Kafka({ clientId: "client-logging-db", brokers: kafkaBrokers });
const consumer = kafka.consumer({ groupId: "client-logging-postgres" });
const deadLetterProducer = kafka.producer();

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => {
    void Promise.all([consumer.disconnect(), deadLetterProducer.disconnect()]).finally(() => pool.end());
  });
}

try {
  await pool.query("SELECT 1 FROM events LIMIT 1");
  await deadLetterProducer.connect();
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
  await consumer.run({
    eachMessage: (record) => processPostgresRecord(pool, deadLetterProducer, record),
  });
} catch (error) {
  console.error("PostgreSQL consumer failed:", error);
  await Promise.all([consumer.disconnect(), deadLetterProducer.disconnect()]);
  await pool.end();
  process.exitCode = 1;
}
