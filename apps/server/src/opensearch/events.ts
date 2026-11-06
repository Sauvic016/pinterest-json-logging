import type { LogEvent } from "../../../../shared/log-event.ts";
import { opensearchUrl } from "../config.ts";

const index = "/client-events";

function put(path: string, body: object): Promise<Response> {
  return fetch(new URL(path, opensearchUrl), {
    method: "PUT",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(body),
    signal: AbortSignal.timeout(5_000),
  });
}

export async function ensureEventIndex(): Promise<void> {
  const response = await put(index, {
    settings: { number_of_replicas: 0 },
    mappings: {
      // Flexible attributes remain in the document. Their text is indexed separately to avoid conflicting field types.
      dynamic: false,
      properties: {
        app: { type: "keyword" },
        app_version: { type: "keyword" },
        event_id: { type: "keyword" },
        event_name: { type: "keyword" },
        event_time: { type: "date" },
        environment: { type: "keyword" },
        platform: { type: "keyword" },
        severity: { type: "keyword" },
        search_text: { type: "text", analyzer: "simple" },
      },
    },
  });
  const body = await response.text();
  if (response.ok) return;
  if (response.status === 400 && JSON.parse(body).error?.type === "resource_already_exists_exception") return;
  throw new Error(`OpenSearch index setup failed (HTTP ${response.status}).`);
}

export async function indexEvent(event: LogEvent): Promise<void> {
  const id = encodeURIComponent(`${event.app}:${event.event_id.toLowerCase()}`);
  const search_text = Object.values(event.attributes).filter((value) => typeof value === "string");
  const response = await put(`${index}/_create/${id}`, { ...event, search_text });
  await response.body?.cancel();
  // A Kafka retry can deliver an event that was already written. Preserve the first copy, as PostgreSQL does.
  if (!response.ok && response.status !== 409) {
    throw new Error(`OpenSearch event indexing failed (HTTP ${response.status}).`);
  }
}
