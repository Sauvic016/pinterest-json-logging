import { createLogger } from "@client-logging-lab/client-logger";
import { scenario } from "./scenarios.ts";
import type { DemoAction } from "./scenarios.ts";

const status = document.querySelector<HTMLParagraphElement>("#status")!;
const inspector = document.querySelector<HTMLAnchorElement>("#inspector")!;
const version = document.querySelector<HTMLSelectElement>("#version")!;
const stop = document.querySelector<HTMLButtonElement>("#stop")!;
const actionButtons = ["success", "failure", "normal", "spike"].map(
  (id) => document.getElementById(id) as HTMLButtonElement,
);
let running = false;
let stopRequested = false;

async function run(actions: DemoAction[]) {
  if (running) return;
  running = true;
  stopRequested = false;
  const activeRun = crypto.randomUUID();
  inspector.href = `/inspector.html?run_id=${encodeURIComponent(activeRun)}`;

  const logger = createLogger({ endpoint: "/v1/events", app: "pin-demo", version: version.value });

  actionButtons.forEach((button) => {
    button.disabled = true;
  });

  version.disabled = true;
  stop.disabled = false;
  let successes = 0;
  let failures = 0;
  let attempted = 0;
  let deliveryFailure = "";
  const deliveries: Promise<void>[] = [];
  try {
    for (const action of actions) {
      if (stopRequested) break;
      const started = performance.now();

      deliveries.push(
        logger
          .log(action.name, {
            level: action.level,
            attributes: { ...action.attributes, run_id: activeRun },
          })
          .then((delivery) => {
            if (delivery.state !== "queued") {
              deliveryFailure = delivery.message;
              console.warn("Diagnostic event delivery was not confirmed:", delivery.message);
            }
          }),
      );

      attempted++;

      if (action.level === "error") failures++;
      else successes++;

      status.textContent = `${attempted}/${actions.length} simulated actions completed. ${successes} succeeded. ${failures} failed.`;

      if (actions.length > 1 && attempted < actions.length) {
        await new Promise((resolve) => setTimeout(resolve, Math.max(0, 1_000 - (performance.now() - started))));
      }
    }
    await logger.close();
    await Promise.all(deliveries);
    const drops = logger.stats();
    status.textContent += stopRequested ? " Run stopped." : " Run complete.";
    status.textContent += ` Delivery drops: ${drops.overflow} queue, ${drops.age} expired, ${drops.retryExhaustion} after retries.`;
    if (deliveryFailure) status.textContent += ` Event delivery failed: ${deliveryFailure}`;
  } catch {
    status.textContent = "The demo could not complete this run. Inspect the browser console.";
  } finally {
    await logger.close();
    running = false;
    stop.disabled = true;
    version.disabled = false;
    actionButtons.forEach((button) => {
      button.disabled = false;
    });
  }
}

document.getElementById("success")!.addEventListener("click", () => {
  void run([
    {
      name: "pin_save_succeeded",
      level: "info",
      attributes: { synthetic: true, duration_ms: 120, outcome: "success" },
    },
  ]);
});

document.getElementById("failure")!.addEventListener("click", () => {
  void run([
    {
      name: "pin_save_failed",
      level: "error",
      attributes: { synthetic: true, duration_ms: 240, outcome: "failure", reason: "simulated_timeout" },
    },
  ]);
});

document.getElementById("normal")!.addEventListener("click", () => {
  void run(scenario("normal"));
});

document.getElementById("spike")!.addEventListener("click", () => {
  void run(scenario("failure"));
});

stop.addEventListener("click", () => {
  stopRequested = true;
  stop.disabled = true;
});
