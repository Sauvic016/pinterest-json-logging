import type { Producer } from "kafkajs";
import type { Pool } from "pg";
import { kafkaDeadLetterTopic } from "../config.ts";
import { insertEvent } from "./events.ts";
import { decodeKafkaRecord } from "../kafka/record.ts";
import type { KafkaRecord } from "../kafka/record.ts";

export async function processPostgresRecord(
  pool: Pool,
  producer: Producer,
  { topic, partition, message }: KafkaRecord,
): Promise<void> {
  const decoded = decodeKafkaRecord(message.value);
  if ("reason" in decoded) {
    const { reason } = decoded;
    await producer.send({
      topic: kafkaDeadLetterTopic,
      acks: -1,
      messages: [
        {
          key: `${topic}:${partition}:${message.offset}`,
          value: JSON.stringify({
            source_topic: topic,
            source_partition: partition,
            source_offset: message.offset,
            reason,
            value_base64: message.value?.toString("base64") ?? null,
          }),
        },
      ],
    });
    console.warn("Kafka event sent to dead-letter topic", { topic, partition, offset: message.offset, reason });
    return;
  }

  await insertEvent(pool, decoded.event);
}
