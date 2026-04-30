# tts-elk — log every The Things Stack event into ELK

A small, **end-to-end runnable** example that subscribes to the streaming
[Events API](https://www.thethingsindustries.com/docs/api/reference/grpc/events/)
of a [The Things Stack](https://www.thethingsindustries.com/docs/) (TTS)
deployment and indexes every event into Elasticsearch, with Kibana on top
for search, dashboards and alerting.

```
The Things Stack ──POST /api/v3/events (SSE)──▶ forwarder ──TCP json──▶ Logstash ──bulk──▶ Elasticsearch ◀── Kibana
```

The forwarder is a ~250-line Python service that opens one streaming
subscription per identifier kind (applications, gateways, organizations,
users, OAuth clients), parses the Server-Sent Events response, and ships
each event as one JSON line to Logstash. Logstash parses the timestamp,
splits the dotted event name, promotes common entity IDs to top-level
fields, and writes to a time-based Elasticsearch data stream governed by
an ILM policy.

> **Status:** demo / lab. See [§10 Going to production](#10-going-to-production) for what to harden before running it for real.

---

## Table of contents

1. [Quickstart against the public test tenant](#1-quickstart-against-the-public-test-tenant)
2. [How it works](#2-how-it-works)
3. [API key permissions](#3-api-key-permissions)
4. [Configuration reference](#4-configuration-reference)
5. [Repository layout](#5-repository-layout)
6. [Verifying the pipeline](#6-verifying-the-pipeline)
7. [What you can do in Kibana](#7-what-you-can-do-in-kibana)
8. [Adapting for your own deployment](#8-adapting-for-your-own-deployment)
9. [Troubleshooting](#9-troubleshooting)
10. [Going to production](#10-going-to-production)
11. [Appendix: useful event names](#11-appendix-useful-event-names)

---

## 1. Quickstart

The repo runs against any The Things Stack deployment — Cloud, Enterprise
on-prem, or Open Source. You provide a hostname and an API key.

### 1.1 Create an API key

1. In the TTS Console of your deployment, open
   `https://<your-tts-host>/console/user-api-keys/add`. (For TTS Cloud
   that's typically `<tenant>.eu1.cloud.thethings.industries` for the
   EU1 cluster, `nam1.cloud.thethings.industries` for North America, etc.)
2. Name it `tts-elk-forwarder`.
3. Pick the rights you need. There are two families — see [§3](#3-api-key-permissions) for the full table:
   - **List rights** (`RIGHT_USER_APPLICATIONS_LIST`, `RIGHT_USER_GATEWAYS_LIST`,
     `RIGHT_USER_ORGANIZATIONS_LIST`, `RIGHT_USER_CLIENTS_LIST`) let the
     forwarder *discover* entities to subscribe to.
   - **Info / traffic-read rights** (`RIGHT_APPLICATION_INFO`,
     `RIGHT_APPLICATION_TRAFFIC_READ`, `RIGHT_GATEWAY_INFO`,
     `RIGHT_GATEWAY_TRAFFIC_READ`, `RIGHT_ORGANIZATION_INFO`) gate which
     events the API actually emits over the stream.
   You need **both** for full auto-discovery. If your key only has the
   info/traffic rights (which is common), the forwarder still works —
   set `STATIC_IDENTIFIERS` in `.env` (see §1.4) to subscribe to a
   hard-coded list.
4. Save and **copy the key now** — TTS shows it only once.

### 1.2 Configure and start

```bash
git clone <this-repo> tts-elk
cd tts-elk
cp .env.example .env

# Set TTS_HOST to your deployment's hostname (no scheme) and
# TTS_API_KEY to the bearer token from step 1.1. Optionally tweak
# ELASTIC_PASSWORD / KIBANA_PASSWORD.
${EDITOR:-vi} .env

# Generate a random Kibana encryption key:
sed -i.bak "s|^KIBANA_ENCRYPTION_KEY=.*|KIBANA_ENCRYPTION_KEY=$(openssl rand -hex 32)|" .env && rm .env.bak

docker compose up -d --build
```

The first `docker compose up` does a lot:

1. Pulls Elasticsearch 8.13, Kibana, and Logstash images.
2. Builds the forwarder image (Python 3.12 + httpx).
3. Starts Elasticsearch and waits for it to report `yellow`/`green`.
4. Runs the one-shot **`setup`** container, which:
   - sets the `kibana_system` password,
   - installs the `tts-events-ilm` lifecycle policy,
   - installs the `tts-events-mappings` component template,
   - installs the `tts-events` index template bound to the data stream.
5. Starts Kibana, Logstash, and finally the forwarder.

Once everything is healthy:

- Kibana → <http://localhost:5601> (log in as `elastic` with the password from `.env`).
- Elasticsearch → <http://localhost:9200> (same credentials).

### 1.3 Import the Kibana data view

In Kibana → **Stack Management → Saved Objects → Import**, upload
[`kibana/data-view.ndjson`](kibana/data-view.ndjson). This creates a data
view called `tts-events` over the index pattern `logs-tts.events-*` with
`@timestamp` as the time field.

Then open **Discover** → choose `tts-events`. Within ~30 seconds of any
device traffic, gateway connect, or console action on the deployment,
events should start appearing.

### 1.4 Subscribing without list rights

If your API key only has `*_INFO` / `*_TRAFFIC_READ` rights — not the
user-level `*_LIST` rights — the forwarder cannot discover entities.
Set `STATIC_IDENTIFIERS` in `.env` to a JSON object keyed by entity kind:

```bash
# in .env
STATIC_IDENTIFIERS={"gateways":[{"gateway_id":"my-gateway"}]}
SUBSCRIBE_KINDS=gateways
```

The forwarder will skip discovery entirely and subscribe to exactly that
identifier set. You can mix kinds:

```
STATIC_IDENTIFIERS={"gateways":[{"gateway_id":"my-gw"}],"applications":[{"application_id":"my-app"}]}
SUBSCRIBE_KINDS=gateways,applications
```

---

## 2. How it works

### 2.1 The TTS Events API

`POST /api/v3/events` is a **streaming gRPC-Gateway endpoint** that the
docs describe in detail at
<https://www.thethingsindustries.com/docs/api/reference/grpc/events/>. The
request body shape:

```json
{
  "identifiers": [
    { "application_ids": { "application_id": "my-app" } }
  ],
  "tail": 0,
  "names": ["/.+/"]
}
```

Important constraints we design around:

- **One identifier kind per request.** The API rejects mixing
  `application_ids` and `gateway_ids` in the same call. The forwarder
  therefore opens one subscription per kind and merges them downstream.
- **Streaming wire format.** Despite the `Content-Type: text/event-stream`
  header, the body is plain newline-delimited JSON: one `{"result": …}`
  envelope per line, blank lines as separators. The forwarder strips the
  envelope and accepts both this NDJSON format and proper SSE `data:`
  framing for forward-compatibility.
- **Visibility-scoped.** The API only emits events whose
  `visibility.rights` set is fully covered by the rights of your API key.
  To see everything, the key needs the rights listed in §3.
- **Optional `names` regex** narrows the event names you receive — handy
  for splitting a high-volume deployment across multiple forwarders, e.g.
  one for `^(ns|gs)\..+` and one for `^as\..+`.

### 2.2 What an event looks like

The fields below are the [`Event`](https://www.thethingsindustries.com/docs/api/reference/grpc/events/)
message; everything except `data` is consistent across event types.

```jsonc
{
  "name": "as.up.forward",                  // dotted hierarchy: component.category.action
  "time": "2026-04-30T11:23:45.123Z",       // canonical event time
  "identifiers": [ { "device_ids": { … } } ],
  "data": { "@type": "type.googleapis.com/ttn.lorawan.v3.ApplicationUp", … },
  "correlation_ids": [ "gs:uplink:01H…", "ns:uplink:01H…" ],
  "origin": "as-0",
  "context": { … server-internal … },
  "visibility": { "rights": ["RIGHT_APPLICATION_TRAFFIC_READ"] },
  "unique_id": "01HXYZ…",                   // ULID — used as the ES doc _id
  "authentication": { "type": "bearer", … },
  "user_agent": "grpc-go/1.61.0",
  "remote_ip": "10.0.0.5"
}
```

After the Logstash filters in [`logstash/pipeline/tts-events.conf`](logstash/pipeline/tts-events.conf)
each document also gains:

| Field | Meaning |
|---|---|
| `event_component`, `event_category`, `event_action` | Parts of `name` (e.g. `as`, `up`, `forward`). |
| `application_id`, `gateway_id`, `device_id`, `dev_eui`, `join_eui`, `organization_id`, `user_id`, `client_id` | Promoted out of `identifiers` for cheap aggregations. |
| `_subscription_kind` | Which forwarder subscription delivered it (`applications` / `gateways` / …). |
| `@timestamp` | Set from the TTS `time` field, not Logstash's wall clock. |

### 2.3 Indexing and retention

Documents land in the data stream **`logs-tts.events-default`**. The ILM
policy `tts-events-ilm` (installed by the setup container) rolls over
backing indices daily or at 20 GiB primary-shard size, force-merges them
in the warm phase, and deletes them after `RETENTION_DAYS` (90 by default).

Document `_id` is the TTS `unique_id`, so a reconnect that re-streams a
recent event produces an idempotent overwrite, not a duplicate.

---

## 3. API key permissions

Two distinct families of rights are involved:

**A. List rights** — needed to discover entities to subscribe to:

| Right                            | Allows                                     |
|----------------------------------|--------------------------------------------|
| `RIGHT_USER_APPLICATIONS_LIST`   | `GET /applications`                        |
| `RIGHT_USER_GATEWAYS_LIST`       | `GET /gateways`                            |
| `RIGHT_USER_ORGANIZATIONS_LIST`  | `GET /organizations`                       |
| `RIGHT_USER_CLIENTS_LIST`        | `GET /clients`                             |

**B. Info / traffic-read rights** — gate which events you actually receive:

| Right                            | Provides visibility for                          |
|----------------------------------|--------------------------------------------------|
| `RIGHT_APPLICATION_INFO`         | Application lifecycle events                     |
| `RIGHT_APPLICATION_TRAFFIC_READ` | AS up/down forward, NS uplink, joins             |
| `RIGHT_GATEWAY_INFO`             | Gateway lifecycle events                         |
| `RIGHT_GATEWAY_TRAFFIC_READ`     | GS connect/disconnect, up/down, status           |
| `RIGHT_GATEWAY_STATUS_READ`      | Gateway status / connection-stats events         |
| `RIGHT_ORGANIZATION_INFO`        | Org lifecycle events                             |
| `RIGHT_USER_INFO`                | User auth/login events                           |
| `RIGHT_CLIENT_INFO`              | OAuth-client events                              |

**For full auto-discovery you need rights from both families.** Note in
particular: having `RIGHT_APPLICATION_INFO` alone does *not* let you
list applications — that requires the user-level `RIGHT_USER_APPLICATIONS_LIST`.

If your API key only has rights from family B (which is common — e.g.
collaborator keys scoped to specific applications), use the
`STATIC_IDENTIFIERS` env var ([§1.4](#14-subscribing-without-list-rights))
to skip discovery and supply the entity list directly.

For tenant-wide visibility, an **admin user** still needs the LIST rights
explicitly granted to the API key — `is_admin: true` does not bypass per-key
right checks.

---

## 4. Configuration reference

All variables live in `.env` (copy from `.env.example`).

| Variable | Default | Purpose |
|---|---|---|
| `TTS_HOST` | (placeholder — must change) | TTS hostname (no scheme). e.g. `<tenant>.eu1.cloud.thethings.industries` for TTS Cloud, or your own host for self-hosted. |
| `TTS_API_KEY` | — | Required. Bearer token created in §1.1. |
| `TTS_INSECURE` | `false` | Set `true` only for self-signed dev TTS. |
| `SUBSCRIBE_KINDS` | `applications,gateways,organizations,users,clients` | Identifier kinds to subscribe to. |
| `EVENT_NAMES_REGEX` | `/.+/` | Filter on event names (TTS regex syntax). |
| `REFRESH_INTERVAL` | `300` | Seconds between entity re-list + stream reopen. |
| `STATIC_IDENTIFIERS` | (empty) | JSON object overriding entity discovery. See §1.4. |
| `LOG_LEVEL` | `INFO` | Forwarder log level (`DEBUG` logs every event name). |
| `STACK_VERSION` | `8.13.4` | ES / Kibana / Logstash image tag. |
| `ELASTIC_PASSWORD` | `change-me-elastic` | `elastic` superuser password. |
| `KIBANA_PASSWORD` | `change-me-kibana` | `kibana_system` service account password. |
| `KIBANA_ENCRYPTION_KEY` | — | Required. ≥32-char random; `openssl rand -hex 32`. |
| `ES_JAVA_OPTS`, `LS_JAVA_OPTS` | `-Xms2g -Xmx2g`, `-Xms512m -Xmx512m` | JVM heap. |
| `ES_MEM_LIMIT`, `KB_MEM_LIMIT`, `LS_MEM_LIMIT` | `4g`, `1g`, `1g` | Container memory caps. |
| `KIBANA_PORT`, `ELASTICSEARCH_PORT` | `5601`, `9200` | Host port mappings. |
| `RETENTION_DAYS` | `90` | ILM delete-phase age. |

---

## 5. Repository layout

```
.
├── README.md                       # this file
├── LICENSE                         # Apache-2.0
├── .env.example                    # configuration template
├── docker-compose.yml              # ES + Kibana + Logstash + setup + forwarder
├── forwarder/
│   ├── Dockerfile
│   ├── requirements.txt
│   └── forwarder.py                # async TTS → Logstash forwarder
├── logstash/
│   ├── config/logstash.yml
│   └── pipeline/tts-events.conf    # parse + enrich + ES output
├── elasticsearch/
│   └── setup.sh                    # one-shot ILM + templates installer
└── kibana/
    └── data-view.ndjson            # importable Kibana data view
```

---

## 6. Verifying the pipeline

```bash
# 1) Stack health
docker compose ps
curl -fsS -u elastic:$ELASTIC_PASSWORD http://localhost:9200/_cluster/health?pretty

# 2) Forwarder is subscribed
docker compose logs -f forwarder
#   expect lines like:
#     [applications] subscribing to N entities
#     [gateways]     subscribing to M entities

# 3) Events landing in Elasticsearch
curl -fsS -u elastic:$ELASTIC_PASSWORD \
  "http://localhost:9200/logs-tts.events-default/_search?pretty&size=1&sort=@timestamp:desc"

# 4) Document count is increasing
watch -n 5 "curl -fsS -u elastic:$ELASTIC_PASSWORD http://localhost:9200/logs-tts.events-default/_count"
```

If you have the `ttn-lw-cli` set up, generating a test event is easy:

```bash
ttn-lw-cli --config tts.yml end-devices get my-app my-device  # any read shows up as a *.read event
```

Otherwise, just clicking around the TTS Console (which goes through the
same APIs) generates plenty of `is.*` events.

---

## 7. What you can do in Kibana

In **Discover**, select the `tts-events` data view and try:

- `event_component : "gs" and event_action : ("connect" or "disconnect")`
  — gateway link health timeline.
- `event_category : "join"` — join activity, with `event_action` telling
  you accept vs reject.
- `event_action : "drop"` — anything the stack dropped (uplinks, downlinks,
  application messages).
- `event_component : "is" and event_category : "oauth"` — OAuth audit
  trail.
- `event_component : "as" and event_category : "up"` — what your
  application actually saw.

Suggested first dashboard panels:

| Panel | Definition |
|---|---|
| Event volume | Date histogram, breakdown by `event_component.keyword`. |
| Top noisy devices | Terms on `device_id.keyword`, size 20. |
| Gateway flap timeline | Filter `name : (gs.gateway.connect or gs.gateway.disconnect)`. |
| Failed downlinks | Filter `event_category : "down" and event_action : "fail"`. |
| Auth events | Filter `event_component : "is" and event_category : ("oauth" or "user")`. |

For alerting, install the X-Pack alerting (free in Basic) and create a
threshold alert on, e.g. `count(name : "gs.gateway.disconnect") > 5 in 10m`.

---

## 8. Adapting for your own deployment

To point this stack at a different TTS deployment (Cloud or self-hosted):

1. Set `TTS_HOST` to the deployment's hostname in `.env`.
2. Create an API key in **that** deployment with the rights from §3 and
   set `TTS_API_KEY`.
3. If the deployment uses a self-signed certificate, set
   `TTS_INSECURE=true`. Do not do this for Cloud or production.
4. `docker compose up -d --build`.

To narrow scope to a single application or gateway, edit `forwarder.py`
to skip the `list_entities` call and hard-code a single identifier — or
set `SUBSCRIBE_KINDS=applications` and rely on the API key only having
rights to one application.

---

## 9. Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Forwarder logs `403` from `/applications` etc. | API key lacks the user-level `RIGHT_USER_*_LIST` rights | Either add those rights, or set `STATIC_IDENTIFIERS` (§1.4) to skip discovery. |
| Forwarder logs `no <kind> visible to api key — sleeping` | Same as above, or genuinely no entities | If the API does grant the rights but you legitimately have no entities of that kind, drop them from `SUBSCRIBE_KINDS`. |
| Events flow but `@timestamp` is "now" | Date filter not matching | Check `time` is present in incoming events: `docker compose logs logstash`. |
| Mapping explosion warnings | Open-ended fields in `data` differing per event name | Already mitigated for `context` / `visibility`. If you have very high cardinality on a specific event's `data`, drop or flatten it in `tts-events.conf`. |
| Duplicate events after a forwarder restart | `unique_id` missing | Make sure the forwarder hasn't been modified to strip the field. |
| `429 Too Many Requests` | Aggressive `REFRESH_INTERVAL` against a tenant with thousands of entities | Increase `REFRESH_INTERVAL` to 1800. |
| Forwarder loops `connection reset by peer` | Idle TCP timeout on a load balancer between forwarder and TTS | Front the deployment with a proxy that holds long-lived streams. TTS Cloud supports this natively. |
| `setup` container exits non-zero on first run | ES not yet reachable, or wrong password in `.env` | `docker compose logs setup`; rerun `docker compose up -d` once the typo is fixed. |

---

## 10. Going to production

Everything in this repo is tuned for a fast first-run on a laptop. None
of it is wrong for production, but several explicit shortcuts need to be
undone before you run this against a real deployment with real data and
real users. The checklist below is grouped by concern.

### 10.1 Security

| Concern | What's wrong in the demo | What to do |
|---|---|---|
| **In-cluster TLS** | ES↔Kibana, ES↔Logstash, ES↔setup all run over plain HTTP. | Generate a CA + per-node certs (`bin/elasticsearch-certutil ca` then `cert`), mount them, set `xpack.security.http.ssl.enabled=true`, and switch `http://elasticsearch:9200` → `https://…` everywhere (compose env, Logstash output, setup script). The cert layout is documented in the official [Elastic Docker docs](https://www.elastic.co/guide/en/elasticsearch/reference/current/docker.html). |
| **Edge TLS for Kibana** | Kibana exposes plain HTTP on `localhost:5601`. | Front it with an SSO-aware reverse proxy (Caddy / Traefik / nginx + oauth2-proxy / Cloudflare Access) terminating TLS and enforcing OIDC. Don't bind 5601 publicly. |
| **Least-privilege ES users** | Logstash and the setup container both authenticate as `elastic` (superuser). | Create dedicated roles via the [Security API](https://www.elastic.co/guide/en/elasticsearch/reference/current/security-api-put-role.html): a `tts-events-writer` role with `create_doc`/`auto_configure` on `logs-tts.events-*` for Logstash, and a `tts-events-reader` for Kibana viewers. Only the setup script needs admin rights. |
| **Secrets management** | `TTS_API_KEY` and `ELASTIC_PASSWORD` live in `.env`, mounted as plain env vars. | Inject from a secrets store (Vault, AWS Secrets Manager, GCP Secret Manager, Doppler, sealed-secrets in K8s). Compose can consume Docker secrets; Kubernetes can mount them as files. |
| **Network isolation** | All services share one bridge network on the host. | In K8s: separate namespaces, NetworkPolicies restricting forwarder → only egress to TTS + the Logstash port; Logstash → only ES; Kibana → only ES. Block lateral traffic. |
| **TTS API key scope** | The README's quickstart asks for tenant-wide rights. | Provision a *service-account*-style user (collaborator-only, no Console login), grant it the minimum rights from §3, and rotate the key on a schedule. Audit the resulting events under `is.api_key.*`. |
| **Encryption at rest** | The Elasticsearch volume is unencrypted on the host. | Use encrypted block storage (LUKS, EBS-encrypted, GCE PD with CMEK). Required for many compliance regimes. |
| **Audit log** | Not enabled. | Turn on the [Elasticsearch audit log](https://www.elastic.co/guide/en/elasticsearch/reference/current/enable-audit-logging.html); ship Kibana access logs from the reverse proxy. |

### 10.2 Reliability and durability

| Concern | What's wrong in the demo | What to do |
|---|---|---|
| **Forwarder → Logstash is a raw TCP socket** | If Logstash dies or restarts, the forwarder's `sendall` blocks; the TTS server-side per-subscriber buffer is bounded and old events are dropped, not retried. | Insert **Kafka or Redpanda** between the forwarder and Logstash. Forwarder writes to a topic with `acks=all`; Logstash's `kafka` input consumes with at-least-once semantics. The TTS `unique_id` → ES `_id` mapping makes any redelivery idempotent. |
| **Single-node Elasticsearch** | One node = no HA, no quorum. A restart pauses ingest. | Run a 3-node cluster with dedicated master + data + (optionally) ingest roles. For managed offerings, use Elastic Cloud or [ECK](https://www.elastic.co/guide/en/cloud-on-k8s/current/index.html) on Kubernetes. |
| **Forwarder is a single instance** | One container = SPOF. | Two patterns: **active-passive** with a leader-election sidecar (k8s `Lease`, etcd), simpler but with a fail-over gap; or **active-active** running multiple replicas — the `unique_id` dedup means duplicates collapse, but you spend 2× the API quota and TTS connections. Pick based on event-loss tolerance. |
| **No snapshots** | The ES volume is a single point of data loss. | Register a snapshot repository (S3 / GCS / Azure / shared FS) and configure [SLM](https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-lifecycle-management.html) to back up daily, with retention matching your compliance window. |
| **Stack restarts lose in-flight data** | The TTS API has a small server-side buffer; a long forwarder outage means lost events. | If you can tolerate slightly higher start-up latency, persist the last `unique_id` you indexed and use the `after` parameter on the Events API to replay from there. (Not implemented here — the buffer is bounded server-side, so this only helps for outages of seconds-to-minutes.) |
| **No backpressure visibility** | Forwarder, Logstash, and ES can each fall behind without alarming. | See [§10.4](#104-observability-of-the-pipeline-itself). |

### 10.3 Scale and performance

The demo is single-node and will keep up with **tens of events/sec**.
Beyond that:

- **Capacity sketch.** Approximate sizes after compression:
  - ~600 B / event for IS / lifecycle events.
  - ~1.5–2 KiB / event for `as.up.forward` (carries the full `ApplicationUp` payload).
  - 10 000 active devices at one uplink / 15 min ≈ 11 events/s from AS,
    ~3× that across NS + GS — plan **~3 GiB/day** at 30 events/s sustained.
- **Logstash.** Tune `pipeline.workers` (= CPU cores) and `pipeline.batch.size`
  (default 125 → try 500–1000). Run multiple Logstash replicas behind a
  load balancer or Kafka consumer group.
- **ES sharding.** The default of 1 primary shard / 1 replica is fine up
  to ~50 GiB/shard. Add primaries if a single shard becomes a bottleneck;
  the ILM policy already rolls over at 20 GiB.
- **Hot/warm/cold tiers.** With more than one data node, set
  `index.routing.allocation.include._tier_preference` per ILM phase to
  put older indices on cheaper hardware.
- **Drop high-volume payload fields.** Most bytes are in `as.up.forward.data`.
  If you don't need decoded payloads in ES (they're often available in
  your application database):
  ```ruby
  if [name] == "as.up.forward" {
    mutate { remove_field => ["[data][frm_payload]", "[data][decoded_payload]"] }
  }
  ```
- **Sampling.** For chatty events that are useful in aggregate but not
  individually (e.g. `gs.up.receive` at 100 ev/s), sample 1-in-N in the
  Logstash filter to cap volume.
- **Filter at the source.** `EVENT_NAMES_REGEX` lets the forwarder scope
  the stream server-side. Splitting by component (one forwarder for
  `^as\..+`, one for `^(ns|gs)\..+`) parallelises ingest and lets you
  scale each axis independently.

### 10.4 Observability of the pipeline itself

If the pipeline silently stops, the absence of events looks the same as
"nothing happened." Add:

- **Stack monitoring.** Run [Metricbeat](https://www.elastic.co/guide/en/beats/metricbeat/current/metricbeat-module-elasticsearch.html)
  or Elastic Agent against ES, Kibana, and Logstash; ship to a separate
  monitoring cluster (or the same cluster with a dedicated data view).
- **Lag alert.** A simple, high-signal alarm:
  ```
  max(now() - @timestamp) over logs-tts.events-* > 5m → page
  ```
  Implement as a Kibana threshold rule. This catches forwarder hangs,
  TTS API outages, and Logstash backpressure with one signal.
- **Forwarder metrics.** Add a Prometheus endpoint exposing event count,
  drop count, current subscription state, and last-event timestamp per
  kind. Not in the demo — the right place to add it is alongside
  `LogstashSink` in `forwarder.py`.
- **Diversity drop.** Alarm when the count of distinct `name`s per
  10 min falls below your usual baseline — that catches "subscription
  stalled but TCP still open" failure modes.

### 10.5 Compliance and governance

TTS events contain **personal data**: `user_id`, `remote_ip`,
`user_agent`, sometimes device identifiers correlated to physical
hardware. Treat the index as a personal-data store.

- **Retention by class.** Different events warrant different retention.
  Admin (`is.*`) and security events: 1+ year. Traffic events: weeks. Set
  `RETENTION_DAYS` to the longer one and use a separate downstream
  pipeline (or a delete-by-query schedule) to purge traffic earlier.
- **Field redaction.** The Logstash filter already drops `authentication`
  and `user_agent`. Consider also pseudonymising `remote_ip` (hash with a
  per-tenant salt) if the IP isn't required for forensics.
- **Access control.** Use Kibana **Spaces** to partition by tenant and
  role-based **document-level security** to restrict who sees what.
  Auditors get read-only on a redacted view.
- **Data subject deletion.** When a user is deleted in TTS, you'll get
  an `is.user.delete` event — pipe that into a delete-by-query against
  `user_id` to honour erasure requests.
- **Data residency.** Make sure your ES cluster lives in the same region
  as the TTS deployment if regulation requires it (EU1 → EU; NAM1 → US).

### 10.6 Multi-tenancy

If you collect events from multiple TTS tenants:

- Run **one forwarder per tenant** with the tenant's own API key.
- Set `data_stream_namespace` per tenant in `logstash/pipeline/tts-events.conf`,
  e.g. `data_stream_namespace => "tenant-acme"` → events land in
  `logs-tts.events-tenant-acme`.
- Use **Kibana Spaces** + role-based DLS to partition who sees which
  tenant's data.
- Roll up cross-tenant dashboards in a dedicated admin space backed by
  an index pattern of `logs-tts.events-*`.

### 10.7 Lifecycle and change management

- **TTS evolution.** New event names are added in nearly every TTS
  release. Because the index template uses `dynamic: "true"`, new fields
  appear automatically — but they may surprise dashboards built on a
  fixed list of names. Check the [TTS release notes](https://www.thethingsindustries.com/docs/whats-new/)
  on upgrade and review the [`pkg/events`](https://github.com/TheThingsNetwork/lorawan-stack/tree/main/pkg/events)
  diff. Pin a known schema version into your CI.
- **Stack upgrades.** Upgrade Elasticsearch, Kibana, and Logstash
  in lockstep. Do a canary upgrade against a non-production index pattern
  before the main cluster.
- **Forwarder upgrades.** The forwarder is stateless, so rolling
  restarts are safe; the TTS server-side buffer covers the disconnect.

### 10.8 Recommended deployment patterns

- **Kubernetes**: use the [Elastic Cloud on Kubernetes (ECK)](https://www.elastic.co/guide/en/cloud-on-k8s/current/index.html)
  operator for ES + Kibana, deploy Logstash and the forwarder as
  Deployments with HPA; everything else is roughly the same as this
  Compose file. See `kibana/data-view.ndjson` for a saved-objects
  example you can apply via [`kibana_saved_objects` API](https://www.elastic.co/guide/en/kibana/current/saved-objects-api.html).
- **Elastic Cloud**: replace the `elasticsearch` and `kibana` services
  with a managed deployment, point Logstash and the forwarder at the
  Cloud endpoints with an API key, and you're done. The setup script
  works unchanged against a managed cluster.
- **Forwarder packaging**: this repo builds a single image. For larger
  fleets, push it to a registry and pin the digest, since events get
  reshaped between versions.

---

## 11. Appendix: useful event names

A non-exhaustive cheat-sheet for filter building. The authoritative list
lives in the TTS source under
[`pkg/events`](https://github.com/TheThingsNetwork/lorawan-stack/tree/v3.30.0/pkg/events)
and per-component event registrations.

| Component | Example event names | What they tell you |
|---|---|---|
| Identity Server (`is`) | `is.user.create`, `is.application.create`, `is.gateway.update`, `is.oauth.access_token.create` | Lifecycle and auth audit. |
| Network Server (`ns`) | `ns.up.receive`, `ns.up.merge_metadata`, `ns.up.join.receive`, `ns.up.join.accept.forward`, `ns.down.schedule.attempt`, `ns.down.schedule.success`, `ns.mac.*` | LoRaWAN MAC layer. |
| Application Server (`as`) | `as.up.forward`, `as.up.drop`, `as.up.data.forward`, `as.down.data.receive`, `as.down.data.forward` | What the application sees. |
| Gateway Server (`gs`) | `gs.gateway.connect`, `gs.gateway.disconnect`, `gs.up.receive`, `gs.down.send`, `gs.status.receive` | Link health. |
| Join Server (`js`) | `js.join.accept`, `js.join.reject` | Joins, including reasons. |
| Packet Broker (`pba`) | `pba.uplink.forward`, `pba.downlink.receive` | Roaming traffic. |

Use these with the parsed prefix fields, e.g.:

```
event_component : "as" and event_category : "up" and event_action : "drop"
```

---

## License

[Apache 2.0](LICENSE).
