CREATE TABLE IF NOT EXISTS events (
  app text NOT NULL,
  event_id uuid NOT NULL,
  schema_version smallint NOT NULL CHECK (schema_version = 1),
  event_time timestamptz NOT NULL,
  received_at timestamptz NOT NULL DEFAULT now(),
  app_version text NOT NULL,
  environment text NOT NULL CHECK (environment = 'demo'),
  platform text NOT NULL CHECK (platform = 'web'),
  event_name text NOT NULL,
  severity text NOT NULL CHECK (severity IN ('info', 'warn', 'error')),
  attributes jsonb NOT NULL CHECK (jsonb_typeof(attributes) = 'object'),
  PRIMARY KEY (app, event_id)
);

CREATE INDEX IF NOT EXISTS events_received_at_idx ON events (received_at DESC);

CREATE INDEX IF NOT EXISTS events_app_received_at_idx ON events (app, received_at DESC);
