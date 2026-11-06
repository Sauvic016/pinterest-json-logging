# Client logging lab

A local demo of a client logging pipeline. The web app simulates Pin saves and failures; the client logger validates and batches events, the API publishes them to Kafka, and separate consumers write them to PostgreSQL and OpenSearch. The inspector and Grafana read stored events. Demo actions are synthetic.

Inspired by Pinterest Engineering's [Build an end to end JSON logging system for clients apps](https://medium.com/pinterest-engineering/build-an-end-to-end-json-logging-system-for-clients-apps-58ad008a1ce9). This repository implements a small-scale local version of that idea.

```text
Demo web app → client logger → Fastify API → Kafka
                                       ├→ PostgreSQL consumer → PostgreSQL → inspector / Grafana
                                       └→ OpenSearch consumer → OpenSearch Dashboards
```

## How this differs from Pinterest's system

| Pinterest's article | This project |
| --- | --- |
| Client logging across app platforms | One browser SDK and a synthetic Pin-save demo |
| Flexible key-value log payloads | A fixed event envelope with validated, flat attributes; the API accepts only `pin-demo` |
| Hive persistence for SQL queries | PostgreSQL, with event IDs used to ignore duplicate writes |
| OpenSearch queries and log-based alerting with Statsboard | OpenSearch Dashboards for search; Grafana reads PostgreSQL for charts and alerts |

Kafka and OpenSearch follow the article's broad pipeline, but this version runs as a small local Docker Compose setup rather than a production logging platform.

## Tech Stack

TypeScript and Node.js, Vite, Fastify, KafkaJS, PostgreSQL 17, OpenSearch, Grafana, and Docker Compose. The demo imports the client logger from a private local npm workspace package.

`packages/client-logger` is a browser SDK used by the demo; keeping it as a private workspace package shows how an app consumes the logger without publishing it. It supports desktop and mobile browsers, but there are no native mobile or desktop SDKs. The event contract currently accepts only `web` demo events. Adding a native client would require its own SDK and an update to the shared event contract and server validation.

## Run locally

Requires Node.js 22.18+ and Docker with Compose. From the repository root:

```sh
npm install
cp .env.example .env
npm run db:up
npm run db:setup
npm run kafka:up
```

Then keep each of these running in its own terminal:

```sh
npm run api
npm run consumer
npm run dev
```

Open the [demo](http://127.0.0.1:5173/) and [event inspector](http://127.0.0.1:5173/inspector.html). The API listens on `127.0.0.1:3001`; `GET /health` reports Kafka readiness. `npm run dev` starts only Vite, so the API terminal must also be running for `/v1/events` requests. If Kafka is down, the API stays up but event POSTs return HTTP 503.

### Optional search and dashboards

For OpenSearch, start Dashboards and its OpenSearch dependency, then run the second consumer in another terminal:

```sh
npm run opensearch:dashboards:up
npm run consumer:opensearch
```

Open [OpenSearch Dashboards](http://127.0.0.1:5601/app/discover). On a fresh instance, create the `client-events` index pattern with `event_time` as its time field.

For Grafana, create its read-only PostgreSQL user once, then start Grafana:

```sh
npm run grafana:setup
npm run grafana:up
```

Open [Grafana](http://127.0.0.1:3000/d/client-logging/client-logging-demo). Sign in as `admin` with `GRAFANA_ADMIN_PASSWORD` from `.env`. Do not rerun `grafana:setup` against a database where `grafana_reader` already exists.

## Repository layout

- `apps/demo-web`: demo and inspector pages.
- `apps/server`: API, destination-specific `postgres` and `opensearch` pipelines, shared Kafka decoding, setup scripts, and event table SQL.
- `apps/grafana`: dashboard and provisioning files.
- `packages/client-logger`: local client logging SDK, imported directly by the demo.
- `shared/log-event.ts`: event type and validation used by the SDK and server.


