import { eventError, jsonBytes, EVENT_LIMITS } from "../../../shared/log-event.ts";
import type { Attributes, LogEvent, Severity } from "../../../shared/log-event.ts";

const DELIVERY_LIMITS = {
  timeoutMs: 8_000,
  queueEvents: 200,
  queueBytes: 256 * 1024,
  maxAgeMs: 5 * 60 * 1000,
  flushMs: 5_000,
  retries: 3,
} as const;

export interface LoggerConfig {
  endpoint: string;
  app: string;
  version: string;
}

export interface Delivery {
  state: "queued" | "rejected" | "unavailable";
  event: LogEvent | null;
  message: string;
  queued: number;
}

type PendingEvent = {
  event: LogEvent;
  bytes: number;
  addedAt: number;
  resolve: (result: Delivery) => void;
};

export class Logger {
  private readonly config: LoggerConfig;
  private readonly transport: typeof fetch;
  private readonly queue: PendingEvent[] = [];
  private activeBatch: PendingEvent[] = [];
  private pendingBytes = 0;
  private flushing: Promise<void> | null = null;
  private closing: Promise<void> | null = null;
  private closed = false;
  private paused = false;
  private readonly dropped = { validation: 0, overflow: 0, age: 0, retryExhaustion: 0 };
  private readonly timer: ReturnType<typeof setInterval>;

  constructor(config: LoggerConfig, transport?: typeof fetch) {
    this.config = config;
    this.transport = transport ?? ((input, init) => globalThis.fetch(input, init));
    this.timer = setInterval(() => {
      if (this.queue.length && !this.flushing && (typeof document === "undefined" || !document.hidden))
        void this.flush();
    }, DELIVERY_LIMITS.flushMs);
    if (typeof this.timer !== "number") this.timer.unref();
    if (typeof document !== "undefined") document.addEventListener("visibilitychange", this.onVisibilityChange);
  }

  async log(name: string, options: { level?: Severity; attributes?: Attributes } = {}): Promise<Delivery> {
    let event: LogEvent | null = null;
    try {
      event = {
        schema_version: 1,
        event_id: crypto.randomUUID(),
        event_time: new Date().toISOString(),
        app: this.config.app,
        app_version: this.config.version,
        environment: "demo",
        platform: "web",
        event_name: name,
        severity: options.level ?? "info",
        attributes: options.attributes ?? {},
      };
      const invalid = eventError(event);
      if (invalid) {
        this.dropped.validation++;
        return { state: "rejected", event, message: invalid, queued: 0 };
      }
      if (this.closed) return { state: "rejected", event, message: "The logger is closed.", queued: 0 };

      // Keep caller mutations from changing already validated, queued data.
      event.attributes = { ...event.attributes };
      return this.enqueue(event);
    } catch {
      return { state: "unavailable", event, message: "Delivery could not be confirmed.", queued: 0 };
    }
  }

  flush(keepalive = false): Promise<void> {
    if (this.flushing) return this.flushing;
    if (this.paused) {
      this.expireWaiting();
      return Promise.resolve();
    }
    this.flushing = this.drainQueue(keepalive).finally(() => {
      this.flushing = null;
    });
    return this.flushing;
  }

  resume(): void {
    if (this.closed) return;
    this.paused = false;
    void this.flush();
  }

  stats() {
    return { ...this.dropped, pending: this.queue.length + this.activeBatch.length };
  }

  close(): Promise<void> {
    if (this.closing) return this.closing;
    clearInterval(this.timer);
    if (typeof document !== "undefined") document.removeEventListener("visibilitychange", this.onVisibilityChange);
    this.closed = true;
    this.closing = this.flush().then(() => {
      if (this.paused) this.settle(this.queue.splice(0), "rejected", "Authorization is unavailable.");
    });
    return this.closing;
  }

  private readonly onVisibilityChange = () => {
    if (document.hidden) void this.flush(true);
  };

  private enqueue(event: LogEvent): Promise<Delivery> {
    this.expireWaiting();
    const bytes = jsonBytes(event);
    while (this.queue.length && this.exceedsQueueLimit(bytes)) {
      this.dropped.overflow++;
      this.settle([this.queue.shift()!], "unavailable", "The pending queue was full.");
    }
    if (this.exceedsQueueLimit(bytes)) {
      this.dropped.overflow++;
      return Promise.resolve({
        state: "unavailable",
        event,
        message: "The pending queue was full.",
        queued: 0,
      });
    }
    return new Promise((resolve) => {
      this.queue.push({ event, bytes, addedAt: Date.now(), resolve });
      this.pendingBytes += bytes;
      if (
        !this.paused &&
        (this.queue.length >= EVENT_LIMITS.batchEvents ||
          jsonBytes({ events: this.queue.map((item) => item.event) }) >= EVENT_LIMITS.batchBytes)
      )
        void this.flush();
    });
  }

  private exceedsQueueLimit(newEventBytes: number): boolean {
    const pendingEvents = this.queue.length + this.activeBatch.length;
    return pendingEvents >= DELIVERY_LIMITS.queueEvents || this.pendingBytes + newEventBytes > DELIVERY_LIMITS.queueBytes;
  }

  private expireWaiting(): void {
    while (this.queue.length && Date.now() - this.queue[0]!.addedAt >= DELIVERY_LIMITS.maxAgeMs) {
      this.dropped.age++;
      this.settle([this.queue.shift()!], "unavailable", "The event expired before upload.");
    }
  }

  private takeBatch(): PendingEvent[] {
    const batch: PendingEvent[] = [];
    while (this.queue.length && batch.length < EVENT_LIMITS.batchEvents) {
      const next = this.queue[0]!;
      const bodyBytes = jsonBytes({ events: [...batch, next].map((item) => item.event) });
      if (bodyBytes > EVENT_LIMITS.batchBytes) break;
      batch.push(this.queue.shift()!);
    }
    return batch;
  }

  private async drainQueue(keepalive: boolean): Promise<void> {
    while (this.queue.length && !this.paused) {
      this.expireWaiting();
      if (!this.queue.length) break;
      this.activeBatch = this.takeBatch();
      try {
        await this.upload(this.activeBatch, keepalive);
      } catch {
        this.settle(this.activeBatch, "unavailable", "Delivery could not be confirmed.");
      } finally {
        this.activeBatch = [];
      }
    }
  }

  private async upload(batch: PendingEvent[], keepalive: boolean): Promise<void> {
    const body = JSON.stringify({ events: batch.map((item) => item.event) });
    const oldestEnqueuedAt = Math.min(...batch.map((item) => item.addedAt));
    const batchExpiresAt = oldestEnqueuedAt + DELIVERY_LIMITS.maxAgeMs;

    for (let attempt = 0; attempt <= DELIVERY_LIMITS.retries; attempt++) {
      const oldestEventTimeLeftMs = batchExpiresAt - Date.now();
      if (oldestEventTimeLeftMs <= 0) {
        this.dropped.age += batch.length;
        this.settle(batch, "unavailable", "The event expired before delivery could be confirmed.");
        return;
      }
      let delay = 1000 * 2 ** attempt + Math.floor(Math.random() * 250);
      try {
        const response = await this.transport(this.config.endpoint, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body,
          signal: AbortSignal.timeout(DELIVERY_LIMITS.timeoutMs),
          credentials: "same-origin",
          keepalive,
        });
        if (response.ok) {
          await this.readReceipt(response, batch.length);
          this.settle(batch, "queued", "Kafka accepted the batch; storage may still be catching up.", batch.length);
          return;
        }
        if (![408, 429, 500, 502, 503, 504].includes(response.status)) {
          if (response.status === 401 || response.status === 403) this.paused = true;
          this.settle(batch, "rejected", `The intake returned HTTP ${response.status}.`);
          return;
        }
        delay = this.retryAfterDelayMs(response.headers.get("Retry-After"), delay);
      } catch (error) {
        console.error(error);
        // A lost response may follow a committed write. Retry with the same event IDs.
      }
      if (attempt === DELIVERY_LIMITS.retries) break;
      if (delay >= batchExpiresAt - Date.now()) {
        this.dropped.age += batch.length;
        this.settle(batch, "unavailable", "The retry delay exceeds the event lifetime.");
        return;
      }
      await new Promise((resolve) => setTimeout(resolve, delay));
    }
    this.dropped.retryExhaustion += batch.length;
    this.settle(
      batch,
      "unavailable",
      "Delivery could not be confirmed after retries. The events may already be queued.",
    );
  }

  private async readReceipt(response: Response, batchSize: number): Promise<void> {
    const receipt: unknown = await response.json();
    if (receipt === null || typeof receipt !== "object" || !("queued" in receipt) || receipt.queued !== batchSize)
      throw new Error("Invalid Kafka receipt.");
  }

  private retryAfterDelayMs(retryAfter: string | null, fallbackDelayMs: number): number {
    if (!retryAfter) return fallbackDelayMs;
    const seconds = Number(retryAfter);
    const requested = Number.isFinite(seconds) ? seconds * 1000 : Date.parse(retryAfter) - Date.now();
    return Number.isFinite(requested) ? Math.max(fallbackDelayMs, requested) : fallbackDelayMs;
  }

  private settle(entries: PendingEvent[], state: Delivery["state"], message: string, queued = 0): void {
    for (const entry of entries) {
      this.pendingBytes -= entry.bytes;
      entry.resolve({ state, event: entry.event, message, queued });
    }
  }
}

export function createLogger(config: LoggerConfig, transport?: typeof fetch): Logger {
  return new Logger(config, transport);
}
