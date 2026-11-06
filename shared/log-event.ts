export const EVENT_LIMITS = {
  eventBytes: 8 * 1024,
  batchBytes: 32 * 1024,
  batchEvents: 20,
  attributes: 20,
} as const;

export type Severity = "info" | "warn" | "error";
export type Attributes = Record<string, string | number | boolean | null>;

export interface LogEvent {
  schema_version: 1;
  event_id: string;
  event_time: string;
  app: string;
  app_version: string;
  environment: "demo";
  platform: "web";
  event_name: string;
  severity: Severity;
  attributes: Attributes;
}

const keys = new Set([
  "schema_version",
  "event_id",
  "event_time",
  "app",
  "app_version",
  "environment",
  "platform",
  "event_name",
  "severity",
  "attributes",
]);

const identifier = /^[a-z][a-z0-9_.-]{0,95}$/;
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i;
const prohibited = /password|secret|token|authorization|cookie|email|^url$|^__proto__$|^constructor$|^prototype$/i;

function isRecord(value: unknown): value is Record<string, unknown> {
  return (
    value !== null &&
    typeof value === "object" &&
    (Object.getPrototypeOf(value) === Object.prototype || Object.getPrototypeOf(value) === null)
  );
}

export function jsonBytes(value: unknown): number {
  return new TextEncoder().encode(JSON.stringify(value)).byteLength;
}

export function eventError(value: unknown): string | null {
  if (!isRecord(value)) return "An event must be a JSON object.";

  if (Object.keys(value).length !== keys.size || Object.keys(value).some((key) => !keys.has(key))) {
    return "The common event fields must match schema version 1.";
  }

  if (value.schema_version !== 1) return "Unsupported schema version.";

  if (typeof value.event_id !== "string" || !uuid.test(value.event_id)) return "Invalid event ID.";

  if (typeof value.event_time !== "string" || !Number.isFinite(Date.parse(value.event_time))) {
    return "Invalid event time.";
  }

  if (new Date(value.event_time).toISOString() !== value.event_time) return "Use an ISO UTC event time.";
  for (const key of ["app", "event_name"]) {
    if (typeof value[key] !== "string" || !identifier.test(value[key])) return `Invalid ${key}.`;
  }
  if (typeof value.app_version !== "string" || !/^[a-zA-Z0-9][a-zA-Z0-9.+_-]{0,63}$/.test(value.app_version)) {
    return "Invalid app version.";
  }
  if (value.environment !== "demo" || value.platform !== "web") return "This intake accepts web demo events only.";

  if (typeof value.severity !== "string" || !["info", "warn", "error"].includes(value.severity))
    return "Invalid severity.";

  if (!isRecord(value.attributes)) return "Attributes must be an object.";

  if (Object.keys(value.attributes).length > EVENT_LIMITS.attributes) return "Too many attributes.";

  for (const [key, item] of Object.entries(value.attributes)) {
    if (!/^[a-z][a-z0-9_]{0,63}$/.test(key) || prohibited.test(key)) return `Attribute name is not permitted: ${key}.`;

    if (item !== null && !["string", "number", "boolean"].includes(typeof item)) return "Use flat scalar attributes.";

    if (typeof item === "number" && !Number.isFinite(item)) return "Numbers must be finite.";

    if (typeof item === "string" && item.length > 2048) return "Attribute text is too long.";
  }
  if (jsonBytes(value) > EVENT_LIMITS.eventBytes) return "Event exceeds 8 KiB.";
  return null;
}

export function batchError(value: unknown): string | null {
  if (!isRecord(value) || Object.keys(value).length !== 1 || !Array.isArray(value.events)) {
    return "Send an object with one events array.";
  }

  if (value.events.length < 1 || value.events.length > EVENT_LIMITS.batchEvents) return "Send between 1 and 20 events.";

  for (const event of value.events) {
    const error = eventError(event);
    if (error) return error;
  }
  if (jsonBytes(value) > EVENT_LIMITS.batchBytes) return "Batch exceeds 32 KiB.";
  return null;
}
