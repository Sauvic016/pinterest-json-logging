import type { Attributes, Severity } from "../../shared/log-event.ts";

export interface DemoAction {
  name: string;
  level: Severity;
  attributes: Attributes;
}

export function scenario(kind: "normal" | "failure", seed = 42): DemoAction[] {

  if (!["normal", "failure"].includes(kind)) throw new Error("Choose a normal or failure scenario.");

  if (!Number.isInteger(seed) || seed < 0 || seed > 0xffffffff)
    throw new Error("Seed must be an unsigned 32-bit integer.");

  let state = seed >>> 0;

  const random = () => {
    state = (Math.imul(1664525, state) + 1013904223) >>> 0;
    return state / 0x100000000;
  };

  const failures = kind === "normal" ? 5 : 30;
  const events: DemoAction[] = Array.from({ length: 100 }, (_, index) => {
    const failed = index < failures;
    return {
      name: failed ? "pin_save_failed" : "pin_save_succeeded",
      level: failed ? "error" : "info",
      attributes: {
        synthetic: true,
        duration_ms: Math.floor(50 + random() * 450),
        outcome: failed ? "failure" : "success",
        reason: failed ? "simulated_timeout" : null,
        diagnostic_group: kind,
      },
    };
  });
  for (let index = events.length - 1; index > 0; index--) {
    const other = Math.floor(random() * (index + 1));
    [events[index], events[other]] = [events[other]!, events[index]!];
  }
  return events;
}
