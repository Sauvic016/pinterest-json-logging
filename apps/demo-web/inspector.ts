import type { LogEvent } from "../../shared/log-event.ts";

const form = document.querySelector<HTMLFormElement>("#filters")!;
const runId = document.querySelector<HTMLInputElement>("#run-id")!;
const status = document.querySelector<HTMLParagraphElement>("#storage-status")!;
const rows = document.querySelector<HTMLTableSectionElement>("#rows")!;
const summaryRows = document.querySelector<HTMLTableSectionElement>("#summary-rows")!;
const detail = document.querySelector<HTMLPreElement>("#detail")!;
let refreshing = false;
let selectedId: string | null = null;
let activeFilter = new URLSearchParams(location.search);
runId.value = activeFilter.get("run_id") ?? "";

async function refresh() {
  if (refreshing) return;
  refreshing = true;
  const filter = activeFilter.toString();
  try {
    const response = await fetch("/v1/events" + (filter ? `?${filter}` : ""), {
      signal: AbortSignal.timeout(8_000),
      cache: "no-store",
    });
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    const data = (await response.json()) as {
      events: (LogEvent & { received_at: string })[];
      summary: { app_version: string; total: number; failures: number }[];
    };
    if (filter !== activeFilter.toString()) return;
    summaryRows.replaceChildren();
    for (const group of data.summary) {
      const row = document.createElement("tr");
      for (const value of [group.app_version, group.total, group.failures]) {
        const cell = document.createElement("td");
        cell.textContent = String(value);
        row.append(cell);
      }
      summaryRows.append(row);
    }
    rows.replaceChildren();
    for (const event of data.events) {
      const row = document.createElement("tr");
      for (const value of [event.received_at, event.event_name, event.app_version, event.severity]) {
        const cell = document.createElement("td");
        cell.textContent = value;
        row.append(cell);
      }
      const idCell = document.createElement("td");
      const select = document.createElement("button");
      select.type = "button";
      select.className = "event-link";
      select.textContent = event.event_id;
      select.setAttribute("aria-label", `Inspect ${event.event_name}, event ${event.event_id}`);
      select.addEventListener("click", () => {
        selectedId = event.event_id;
        detail.textContent = JSON.stringify(event, null, 2);
      });
      idCell.append(select);
      row.append(idCell);
      rows.append(row);
    }
    if (selectedId && !data.events.some((event) => event.event_id === selectedId)) {
      selectedId = null;
      detail.textContent = "The selected event is outside the current results. Select another event.";
    }
    status.textContent = `${data.events.length} stored events in the latest results${filter ? " for the selected filters" : ""}. Updated ${new Date().toLocaleTimeString()}.`;
  } catch {
    status.textContent =
      "Events could not be loaded. Check the API, consumer, and PostgreSQL. Any rows below are from an earlier successful request.";
  } finally {
    refreshing = false;
    if (filter !== activeFilter.toString()) void refresh();
  }
}

form.addEventListener("submit", (event) => {
  event.preventDefault();
  const next = new URLSearchParams();
  for (const name of ["run_id", "app_version", "event_name", "severity", "since", "until"]) {
    const input = form.elements.namedItem(name) as HTMLInputElement;
    const value = input.value.trim();
    if (value) next.set(name, name === "since" || name === "until" ? new Date(value).toISOString() : value);
  }
  activeFilter = next;
  selectedId = null;
  rows.replaceChildren();
  summaryRows.replaceChildren();
  detail.textContent = "Select an event from the table.";
  void refresh();
});
setInterval(() => {
  if (!document.hidden) void refresh();
}, 5_000);
document.addEventListener("visibilitychange", () => {
  if (!document.hidden) void refresh();
});
void refresh();
