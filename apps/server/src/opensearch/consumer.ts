import { Kafka } from "kafkajs";
import { kafkaBrokers, kafkaTopic } from "../config.ts";
import { decodeKafkaRecord } from "../kafka/record.ts";
import { ensureEventIndex, indexEvent } from "./events.ts";

const groupId = "client-logging-opensearch";
const kafka = new Kafka({ clientId: groupId, brokers: kafkaBrokers });
const consumer = kafka.consumer({ groupId });

function reportFailure(error: unknown): void {
  console.error("OpenSearch consumer failed:", error);
  process.exitCode = 1;
}

consumer.on(consumer.events.CRASH, ({ payload }) => {
  if (!payload.restart) reportFailure(payload.error);
});

for (const signal of ["SIGINT", "SIGTERM"] as const) {
  process.once(signal, () => { void consumer.disconnect().catch(reportFailure); });
}

try {
  await ensureEventIndex();
  await consumer.connect();
  await consumer.subscribe({ topic: kafkaTopic, fromBeginning: true });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const decoded = decodeKafkaRecord(message.value);
      if ("reason" in decoded) {
        // The PostgreSQL group owns the shared dead-letter topic so each bad record has one reporting path.
        console.warn("Skipping invalid OpenSearch event", { topic, partition, offset: message.offset, reason: decoded.reason });
        return;
      }
      await indexEvent(decoded.event);
    },
  });
} catch (error) {
  reportFailure(error);
  await consumer.disconnect().catch(reportFailure);
}
