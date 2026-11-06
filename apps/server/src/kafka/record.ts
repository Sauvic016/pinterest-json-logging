import { eventError } from "../../../../shared/log-event.ts";
import type { LogEvent } from "../../../../shared/log-event.ts";

export type KafkaRecord = { topic: string; partition: number; message: { offset: string; value: Buffer | null } };

export function decodeKafkaRecord(value: Buffer | null): { event: LogEvent } | { reason: string } {
  if (value === null) return { reason: "Kafka event has no value." };
  let event: unknown;
  try {
    event = JSON.parse(value.toString("utf8"));
  } catch {
    return { reason: "Kafka event is not valid JSON." };
  }
  const error = eventError(event);
  if (error) return { reason: error };
  const validated = event as LogEvent;
  return validated.app === "pin-demo" ? { event: validated } : { reason: "Unexpected app." };
}
