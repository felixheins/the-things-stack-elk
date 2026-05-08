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

> **Status:** starting recommendation, not a packaged product. Single-node, plain HTTP, shared credentials. Treat it as a reference for building your own deployment — see [§10](#10-beyond-the-demo) for what to think through before running it against real data.
>
> Looking for a self-contained article (no clone needed, all config inline)? See [`docs/logging-events-to-elk.md`](docs/logging-events-to-elk.md).

---

## Table of contents

1. [Quickstart against the public test tenant](#1-quickstart)
2. [How it works](#2-how-it-works)
3. [API key permissions](#3-api-key-permissions)
4. [Configuration reference](#4-configuration-reference)
5. [Repository layout](#5-repository-layout)
6. [Verifying the pipeline](#6-verifying-the-pipeline)
7. [What you can do in Kibana](#7-what-you-can-do-in-kibana)
8. [Common adaptations](#8-common-adaptations)
9. [Troubleshooting](#9-troubleshooting)
10. [Beyond the demo](#10-beyond-the-demo)
11. [Appendix: useful event names](#11-appendix-useful-event-names)

---

## 1. Quickstart

The repo runs against any The Things Stack deployment — Cloud, Enterprise
on-prem, or Open Source. You provide a hostname and an API key.

### 1.1 Create an API key

1. In the TTS Console of your deployment, open
   `https://<your-tts-host>/console/user-settings/api-keys`. (For TTS Cloud
   that's typically `<tenant>.eu1.cloud.thethings.industries` for the
   EU1 cluster, `nam1.cloud.thethings.industries` for North America, etc.)
2. Name it `tts-elk-forwarder`.
3. Pick the rights you need. There are two families — see [§3](#3-api-key-permissions) for the full table:
   - **Discovery rights** — _list applications the user is a collaborator
     of_, _list gateways the user is a collaborator of_, _list
     organizations the user is a member of_, _list OAuth clients the
     user is a collaborator of_, plus _view devices in application_
     (per application, for end-device listing) — let the forwarder
     _discover_ entities to subscribe to.
   - **Visibility rights** — _view application information_, _read
     application traffic (uplink and downlink)_, _view devices in
     application_, _view gateway information_, _read gateway traffic_,
     _view gateway status_, _view organization information_, _view user
     information_, _view OAuth client information_ — gate which events
     the API actually emits over the stream.
     You need **both** for full auto-discovery. If your key only has the
     visibility rights (which is common), the forwarder still works —
     set `STATIC_IDENTIFIERS` in `.env` (see [§1.4](#14-subscribing-without-list-rights)) to subscribe to a
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

The first `docker compose up` does the following:

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
- Elasticsearch → <http://localhost:9200> (log in as `kibana_system` with the password from `.env`).

### 1.3 Import the Kibana saved objects

In Kibana → **Stack Management → Saved Objects → Import**, upload
[`kibana/saved-objects.ndjson`](kibana/saved-objects.ndjson). This
imports:

- A data view `tts-events` over `logs-tts.events-*` (`@timestamp` as the
  time field).
- Five saved searches as starting points for your own dashboards —
  _Gateway link health_, _Joins_, _Drops_, _Auth and audit_, and
  _Application uplinks_. They map to the patterns in [§7](#7-what-you-can-do-in-kibana).

Then open **Discover** → choose `tts-events` (or any of the saved
searches). Within ~30 seconds of any device traffic, gateway connect, or
console action on the deployment, events should start appearing.

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

```bash
STATIC_IDENTIFIERS={"gateways":[{"gateway_id":"my-gw"}],"applications":[{"application_id":"my-app"}]}
SUBSCRIBE_KINDS=gateways,applications
```

For end devices use the `EndDeviceIdentifiers` shape (device id plus the
parent application):

```bash
STATIC_IDENTIFIERS={"end_devices":[{"device_id":"my-dev","application_ids":{"application_id":"my-app"}}]}
SUBSCRIBE_KINDS=end_devices
```

This is the case where `end_devices` carries its weight — typically a
key issued to a per-device collaborator that has no application-list or
application-info rights at all.

---

## 2. How it works

### 2.1 The TTS Events API

`POST /api/v3/events` is a **streaming gRPC-Gateway endpoint** that the
docs describe in detail at
<https://www.thethingsindustries.com/docs/api/reference/grpc/events/>. The
request body shape:

```json
{
  "identifiers": [{ "application_ids": { "application_id": "my-app" } }],
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
  To see everything, the key needs the rights listed in [§3](#3-api-key-permissions).
- **Hierarchical identifier matching.** A subscription on `application_ids`
  also delivers events scoped to the application's _devices_ — TTS treats
  end-device events as covered by their parent application. The forwarder
  therefore captures device traffic via the `applications` subscription
  even without a separate device-level subscription. It still subscribes
  to `end_devices` by default for completeness, mainly so device-scoped
  API keys (no application-list rights) can stream traffic via
  `STATIC_IDENTIFIERS`. Duplicates that arise from this are collapsed in
  Elasticsearch via the `unique_id`-based document ID.
- **Optional `names` regex** narrows the event names you receive — handy
  for splitting a high-volume deployment across multiple forwarders, e.g.
  one for `^(ns|gs)\..+` and one for `^as\..+`.

### 2.2 Load on TTS

The forwarder uses two distinct call patterns against the Events API,
and that's it — there's no general polling loop:

- **Persistent event streams.** One long-lived `POST /api/v3/events`
  per kind in `SUBSCRIBE_KINDS` (up to six by default — applications,
  gateways, organizations, users, clients, end-devices). Events arrive
  pushed; the connection stays open with no read timeout. Reconnects
  back off exponentially up to 60 s on failure.
- **Periodic entity re-discovery, every `REFRESH_INTERVAL` seconds**
  (default `300` = 5 min). At each tick the forwarder paginates
  `GET /<kind>` per kind, then closes and reopens the corresponding
  stream so newly-created entities start being captured. End-device
  discovery also walks `GET /applications/{id}/devices` per
  application, so its cost scales with the application count.

`STATIC_IDENTIFIERS` skips re-discovery entirely — only the persistent
streams remain. On tenants with thousands of entities, raise
`REFRESH_INTERVAL` (e.g. to `1800`) if you see `429 Too Many Requests`,
or drop kinds you don't need from `SUBSCRIBE_KINDS`.

### 2.3 What an event looks like

The fields below are the [`Event`](https://www.thethingsindustries.com/docs/api/reference/grpc/events/)
message; everything except `data` is consistent across event types.

```jsonc
{
  "name": "as.up.data.forward",             // dotted hierarchy — component, category, …, action
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

| Field                                                                                                         | Meaning                                                                      |
| ------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------------- |
| `event_component`, `event_category`, `event_action`                                                           | Parts of `name` (e.g. `as`, `up`, `forward`).                                |
| `application_id`, `gateway_id`, `device_id`, `dev_eui`, `join_eui`, `organization_id`, `user_id`, `client_id` | Promoted out of `identifiers` for cheap aggregations.                        |
| `_subscription_kind`                                                                                          | Which forwarder subscription delivered it (`applications` / `gateways` / …). |
| `@timestamp`                                                                                                  | Set from the TTS `time` field, not Logstash's wall clock.                    |

### 2.4 Indexing and retention

Documents land in the data stream **`logs-tts.events-default`**. The ILM
policy `tts-events-ilm` (installed by the setup container) rolls over
backing indices daily or at 20 GiB primary-shard size, force-merges them
in the warm phase, and deletes them after `RETENTION_DAYS` (90 by default).

Document `_id` is the TTS `unique_id`, so a reconnect that re-streams a
recent event produces an idempotent overwrite, not a duplicate.

---

## 3. API key permissions

Two distinct families of rights are involved. The names below are the
human-readable labels shown in the TTS Console when creating an API
key.

**A. Discovery rights** — needed to discover entities to subscribe to:

| Right                                                | Allows                                |
| ---------------------------------------------------- | ------------------------------------- |
| _list applications the user is a collaborator of_   | `GET /applications`                   |
| _list gateways the user is a collaborator of_       | `GET /gateways`                       |
| _list organizations the user is a member of_        | `GET /organizations`                  |
| _list OAuth clients the user is a collaborator of_  | `GET /clients`                        |
| _view devices in application_ (per application)     | `GET /applications/{id}/devices`      |

The first four are user-level (or organization-level for an
organization API key). _view devices in application_ is per-application,
and is only required if `end_devices` is in `SUBSCRIBE_KINDS` and you
rely on auto-discovery rather than `STATIC_IDENTIFIERS`.

`SUBSCRIBE_KINDS` also includes `users` by default. Listing user
accounts (`GET /users`) requires _list user accounts_, which the
identity server gates behind admin status — selecting it on a normal
API key has no effect. On non-admin keys, drop `users` from
`SUBSCRIBE_KINDS` (otherwise the forwarder logs a WARNING per refresh
interval and skips the kind).

**B. Visibility rights** — gate which events the stream actually emits:

| Right                                                | Provides visibility for                  |
| ---------------------------------------------------- | ---------------------------------------- |
| _view application information_                       | Application lifecycle events             |
| _read application traffic (uplink and downlink)_     | AS up/down forward, NS uplink, joins     |
| _view devices in application_                        | Device CRUD; some uplink/join events     |
| _view gateway information_                           | Gateway lifecycle events                 |
| _read gateway traffic_                               | GS connect/disconnect, up/down           |
| _view gateway status_                                | Gateway status / connection-stats events |
| _view organization information_                      | Org lifecycle events                     |
| _view user information_                              | User auth / login events                 |
| _view OAuth client information_                      | OAuth-client lifecycle events            |

The following are optional — they only add visibility for events
about the corresponding settings change (which the forwarder will
otherwise silently miss):

- _edit basic application settings_, _view and edit application API keys_,
  _view and edit application collaborators_, _view and edit application
  packages and associations_
- _edit basic gateway settings_, _view and edit gateway API keys_,
  _view and edit gateway collaborators_, _view gateway location_
- _edit basic organization settings_, _view and edit organization API
  keys_, _view and edit organization members_
- _edit OAuth client basic settings_, _view and edit OAuth client
  collaborators_
- _view and edit user API keys_, _view and edit authorized OAuth
  clients of the user_

The TTS Console labels the settings rights with "edit", but they are
read+write — TTS does not split read and write for those. There is no
way to get the change events without granting them.

**For full auto-discovery you need rights from both families.** Note
in particular: having _view application information_ alone does _not_
let you list applications — that requires the user-level _list
applications the user is a collaborator of_.

If your API key only has rights from family B (which is common — e.g.
collaborator keys scoped to specific applications), use the
`STATIC_IDENTIFIERS` env var ([§1.4](#14-subscribing-without-list-rights))
to skip discovery and supply the entity list directly.

For tenant-wide visibility, an **admin user** still needs the discovery
rights explicitly granted to the API key — `is_admin: true` does not
bypass per-key right checks. The exception is _list user accounts_,
which is admin-gated regardless.

What to deliberately leave **out**: any right with _delete_, _purge_,
_create_, _write_, or _link_ in its label — the forwarder is read-only.
Also _view device keys in application_ and _retrieve secrets associated
with a gateway_: no event declares these as visibility rights, and
granting them only exposes secrets in listing responses, which you
don't want flowing into ELK.

---

## 4. Configuration reference

All variables live in `.env` (copy from `.env.example`).

| Variable                                       | Required | Default                                                         | Purpose                                                                                                                   |
| ---------------------------------------------- | -------- | --------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `TTS_HOST`                                     | Required | —                                                               | TTS hostname (no scheme). e.g. `<tenant>.eu1.cloud.thethings.industries` for TTS Cloud, or your own host for self-hosted. |
| `TTS_API_KEY`                                  | Required | —                                                               | Bearer token created in [§1.1](#11-create-an-api-key).                                                                    |
| `TTS_INSECURE`                                 | Optional | `false`                                                         | Set `true` only for self-signed dev TTS.                                                                                  |
| `SUBSCRIBE_KINDS`                              | Optional | `applications,gateways,organizations,users,clients,end_devices` | Identifier kinds to subscribe to.                                                                                         |
| `EVENT_NAMES_REGEX`                            | Optional | `/.+/`                                                          | Filter on event names (TTS regex syntax).                                                                                 |
| `REFRESH_INTERVAL`                             | Optional | `300`                                                           | Seconds between entity re-list + stream reopen.                                                                           |
| `STATIC_IDENTIFIERS`                           | Optional | (empty)                                                         | JSON object overriding entity discovery. See [§1.4](#14-subscribing-without-list-rights).                                 |
| `LOG_LEVEL`                                    | Optional | `INFO`                                                          | Forwarder log level (`DEBUG` logs every event name).                                                                      |
| `STACK_VERSION`                                | Required | `8.13.4`                                                        | ES / Kibana / Logstash image tag.                                                                                         |
| `ELASTIC_PASSWORD`                             | Required | `change-me-elastic`                                             | `elastic` superuser password.                                                                                             |
| `KIBANA_PASSWORD`                              | Required | `change-me-kibana`                                              | `kibana_system` service account password.                                                                                 |
| `KIBANA_ENCRYPTION_KEY`                        | Required | —                                                               | ≥32-char random; `openssl rand -hex 32`.                                                                                  |
| `ES_JAVA_OPTS`, `LS_JAVA_OPTS`                 | Required | `-Xms2g -Xmx2g`, `-Xms512m -Xmx512m`                            | JVM heap.                                                                                                                 |
| `ES_MEM_LIMIT`, `KB_MEM_LIMIT`, `LS_MEM_LIMIT` | Required | `4g`, `1g`, `1g`                                                | Container memory caps.                                                                                                    |
| `KIBANA_PORT`, `ELASTICSEARCH_PORT`            | Required | `5601`, `9200`                                                  | Host port mappings.                                                                                                       |
| `RETENTION_DAYS`                               | Required | `90`                                                            | ILM delete-phase age.                                                                                                     |
| `ES_HOSTS`                                     | Optional | `http://elasticsearch:9200`                                     | Logstash → ES endpoint. Override to target an external / managed cluster ([§8.1](#81-pointing-at-an-external-or-managed-elasticsearch)). |
| `ES_USER`, `ES_PASSWORD`                       | Optional | `elastic`, `${ELASTIC_PASSWORD}`                                | Logstash → ES credentials.                                                                                                |
| `DATA_STREAM_NAMESPACE`                        | Optional | `default`                                                       | ES data-stream namespace; set per tenant for multi-tenant ingest ([§8.2](#82-multi-tenant-ingest-one-cluster-many-tts-deployments)). |

---

## 5. Repository layout

```
.
├── README.md                          # this file
├── LICENSE                            # Apache-2.0
├── .env.example                       # configuration template
├── .gitignore
├── docker-compose.yml                 # ES + Kibana + Logstash + setup + forwarder
├── docs/
│   └── logging-events-to-elk.md       # standalone, self-contained article (no clone needed)
├── forwarder/
│   ├── Dockerfile                     # python:3.12-slim base
│   ├── requirements.txt               # httpx
│   └── forwarder.py                   # async TTS → Logstash forwarder, with /healthz endpoint
├── logstash/
│   ├── config/logstash.yml            # disables xpack monitoring, sets ECS v8
│   └── pipeline/tts-events.conf       # parse name + identifiers, enrich, write to data stream
├── elasticsearch/
│   └── setup.sh                       # one-shot ILM policy + component & index templates
└── kibana/
    └── saved-objects.ndjson           # importable data view + 5 saved searches
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
same APIs) generates plenty of identity-server events
(`application.*`, `gateway.*`, `user.*`, `oauth.*`).

---

## 7. What you can do in Kibana

The saved-objects import in [§1.3](#13-import-the-kibana-saved-objects) ships five Discover searches that line
up with the most common questions:

| Saved search              | Filter                                                                                          |
| ------------------------- | ----------------------------------------------------------------------------------------------- |
| TTS — Gateway link health | `event_component : "gs" and event_action : (connect or disconnect)`                             |
| TTS — Joins               | `name : *join*` (KQL wildcard — covers `js.join.*`, `as.up.join.*`, `ns.up.join.*`, `ns.down.join.*`) |
| TTS — Drops               | `event_action : "drop"` (matches every `*.drop` event — uplinks, downlinks, app messages, …)    |
| TTS — Auth and audit      | `name : (oauth.* or account.* or user.* or invitation.* or *.api-key.* or *.collaborator.*)`    |
| TTS — Application uplinks | `name : "as.up.data.forward"`                                                                   |

How the parsed-name fields work: the Logstash filter splits `name` on
dots and exposes the **first** segment as `event_component`, the
**second** as `event_category`, and the **last** as `event_action`. So
`as.up.data.forward` becomes `as` / `up` / `forward`, and
`gs.down.schedule.fail` becomes `gs` / `down` / `fail`. Two-segment
names like `application.create` set component + category + action all
to non-empty values (component=`application`, category=`create`,
action=`create`).

These are intentionally just queries with column presets — extend them or
build Lens visualisations on top. A typical first dashboard pulls in:

| Panel                 | Definition                                                  |
| --------------------- | ----------------------------------------------------------- |
| Event volume          | Date histogram, breakdown by `event_component`.             |
| Top noisy devices     | Terms on `device_id`, size 20.                              |
| Gateway flap timeline | The _Gateway link health_ saved search above.               |
| Failed downlinks      | Filter `event_category : "down" and event_action : "fail"`. |
| Auth events           | The _Auth and audit_ saved search above.                    |

(The pre-mapped `keyword` fields in [`elasticsearch/setup.sh`](elasticsearch/setup.sh)
do **not** have a `.keyword` subfield — use the field name directly in
Lens / TSVB.)

### 7.1 An example alert rule — ingest lag

The single highest-signal alarm is "no events have been indexed in the
last few minutes": it catches forwarder hangs, TTS API outages, and
Logstash backpressure with one rule. Create it in Kibana
**Stack Management → Rules** as an _Elasticsearch query_ rule, or via
the Alerting API:

```bash
curl -fsS -u elastic:$ELASTIC_PASSWORD \
  -H 'Content-Type: application/json' -H 'kbn-xsrf: true' \
  -X POST http://localhost:5601/api/alerting/rule \
  -d @- <<'JSON'
{
  "name": "TTS events ingest lag",
  "tags": ["tts-elk"],
  "rule_type_id": ".es-query",
  "consumer": "alerts",
  "schedule": { "interval": "1m" },
  "params": {
    "searchType": "esQuery",
    "index": ["logs-tts.events-*"],
    "timeField": "@timestamp",
    "esQuery": "{\"query\":{\"match_all\":{}}}",
    "size": 0,
    "thresholdComparator": "<",
    "threshold": [1],
    "timeWindowSize": 5,
    "timeWindowUnit": "m"
  },
  "actions": []
}
JSON
```

The rule fires when fewer than 1 document is indexed in the last 5
minutes. Wire it to a connector (Slack, email, PagerDuty, …) by adding
entries to the empty `"actions"` array; see the
[Kibana Alerting docs](https://www.elastic.co/guide/en/kibana/current/create-and-manage-rules.html)
for the connector schema.

---

## 8. Common adaptations

This is a knowledge-base example, not a packaged product — every
deployment will tweak it. The variants below show the smallest diff for
the cases that come up most often. Each one is independent; combine as
needed.

### 8.1 Pointing at an external or managed Elasticsearch

The Logstash output is fully env-driven, so targeting a managed cluster
needs no code change. In `.env`:

```bash
ES_HOSTS=https://my-deployment.es.eu-west-1.aws.found.io:9243
ES_USER=elastic
ES_PASSWORD=your-cloud-password
```

Then `docker compose up -d logstash forwarder` — the bundled
`elasticsearch` and `kibana` services become unused. The `setup`
container expects to talk to the in-stack ES, so for an external cluster
either install the ILM policy + index template manually (the curl calls
in [`elasticsearch/setup.sh`](elasticsearch/setup.sh) work as-is against
any cluster — set `ES` and `AUTH` to your endpoint) or let the data stream
auto-create with default mappings and add the template after the fact.

For self-managed clusters with a private CA, mount the CA into the
Logstash container and add `cacert => "/path/to/ca.pem"` next to the
`hosts` line in [`logstash/pipeline/tts-events.conf`](logstash/pipeline/tts-events.conf).

### 8.2 Multi-tenant ingest (one cluster, many TTS deployments)

Run **one forwarder per TTS tenant** with the tenant's own API key, and
set `DATA_STREAM_NAMESPACE` per tenant so events land in distinct data
streams:

```bash
# tenant A
DATA_STREAM_NAMESPACE=tenant-acme   # → logs-tts.events-tenant-acme
# tenant B
DATA_STREAM_NAMESPACE=tenant-globex # → logs-tts.events-tenant-globex
```

Use **Kibana Spaces** + role-based document-level security to partition
who sees which tenant's data. A cross-tenant admin space backed by
`logs-tts.events-*` rolls up the lot.

### 8.3 Trimming high-volume payloads

Most bytes in `as.up.data.forward` events are in the decoded/raw payload.
If your application database already has these — which is usually the
case — drop them in the Logstash filter:

```ruby
# Add to logstash/pipeline/tts-events.conf, inside filter { … }
if [name] == "as.up.data.forward" {
  mutate {
    remove_field => ["[data][frm_payload]", "[data][decoded_payload]"]
  }
}
```

Combined with a tightened `EVENT_NAMES_REGEX` (e.g. `^(?!.*\.up\.).+`
to skip uplinks entirely) this routinely cuts ingest by 5–10×.

### 8.4 Narrowing scope to a single entity

Two ways:

- **Via API key.** Create a key with rights to one application, leave
  `STATIC_IDENTIFIERS` empty, set `SUBSCRIBE_KINDS=applications`. The
  forwarder will discover exactly the entities the key can see.
- **Via STATIC_IDENTIFIERS.** Hard-code the targets — useful when the
  key has visibility rights (info / traffic-read / devices-read) but not
  the user-level _list ... the user is a collaborator of_ rights needed
  for discovery ([§1.4](#14-subscribing-without-list-rights)).

---

## 9. Troubleshooting

| Symptom                                                  | Likely cause                                                                                                                  | Fix                                                                                                                                                      |
| -------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Forwarder logs `api key invalid or expired (401 …)` (ERROR) | API key revoked, deleted, or never accepted by TTS                                                                          | Mint a new key ([§1.1](#11-create-an-api-key)), update `TTS_API_KEY` in `.env`, `docker compose up -d --build forwarder`. Note the forwarder retries forever — it does **not** fail-fast on 401. |
| Forwarder logs `api key has no rights to list <kind> — skipping` (WARNING) | API key lacks the user-level _list ... the user is a collaborator of_ right for that kind (see [§3](#3-api-key-permissions))  | Either add the right, or drop the kind from `SUBSCRIBE_KINDS`, or set `STATIC_IDENTIFIERS` ([§1.4](#14-subscribing-without-list-rights)) to skip discovery. |
| Forwarder logs `no <kind> visible to api key — sleeping` | Key has the list right but no entities of that kind exist (or are visible to it)                                              | Drop the kind from `SUBSCRIBE_KINDS` if you legitimately have none.                                                                                      |
| Events flow but `@timestamp` is "now"                    | Date filter not matching                                                                                                      | Check `time` is present in incoming events: `docker compose logs logstash`.                                                                              |
| Mapping explosion warnings                               | Open-ended fields in `data` differing per event name                                                                          | Already mitigated for `context` / `visibility`. If you have very high cardinality on a specific event's `data`, drop or flatten it in `tts-events.conf`. |
| Duplicate events after a forwarder restart               | `unique_id` missing                                                                                                           | Make sure the forwarder hasn't been modified to strip the field.                                                                                         |
| `429 Too Many Requests`                                  | Aggressive `REFRESH_INTERVAL` against a tenant with thousands of entities                                                     | Increase `REFRESH_INTERVAL` to 1800.                                                                                                                     |
| Forwarder loops `connection reset by peer`               | Idle TCP timeout on a load balancer between forwarder and TTS                                                                 | Front the deployment with a proxy that holds long-lived streams. TTS Cloud supports this natively.                                                       |
| `setup` container exits non-zero on first run            | ES not yet reachable, or wrong password in `.env`                                                                             | `docker compose logs setup`; rerun `docker compose up -d` once the typo is fixed.                                                                        |
| `docker compose ps` shows `forwarder` as `unhealthy`     | The forwarder's `/healthz` endpoint is unreachable or its in-memory heartbeat is older than 90 s. Either the process is wedged or the container is starting up. | Check `docker compose logs forwarder`. The healthcheck has `start_period: 60s` so the first ~minute after `up` is allowed to be unhealthy. Probe by hand with `docker compose exec forwarder python -c "import urllib.request; print(urllib.request.urlopen('http://localhost:8080/healthz').read())"`. |

---

## 10. Beyond the demo

This repo is a **starting recommendation**, not a packaged product.
It runs single-node, over plain HTTP, with shared credentials, with
no buffer between the forwarder and the indexer. Every one of those
choices is fine for a laptop and wrong for production. Before running
it against real data, work through the checklist below — the specifics
depend on your environment, so treat each item as a question to answer
rather than a recipe to copy.

**Security**

- Enable TLS on every hop. Don't expose Kibana on a public port without
  an authenticating proxy in front of it.
- Replace shared `elastic`-superuser credentials with least-privilege
  service roles. Only the one-shot setup container needs admin rights.
- Inject `TTS_API_KEY` and storage passwords from a secret store, not
  from `.env`.
- Restrict network paths so each service only reaches what it must.
- Encrypt the storage volume.
- Enable the storage tier's audit log, and ship access logs from your
  proxy.

**Reliability**

- The forwarder→Logstash hop is a raw TCP socket. Insert a durable
  at-least-once queue between the forwarder and the indexer so that an
  indexer outage doesn't translate to event loss. The `unique_id` → ES
  `_id` mapping makes any redelivery idempotent.
- Run the storage tier in HA. Schedule snapshot-based backups with
  retention matching your compliance window.
- The forwarder is stateless and idempotent end-to-end, so multiple
  replicas can run active-active. Pick active-active or active-passive
  based on event-loss tolerance — active-active doubles API quota and
  TTS connections.
- The TTS server-side buffer for events is bounded; long forwarder
  outages drop old events on the server. Keep forwarder downtime
  short, or implement a replay strategy keyed on `unique_id`.

**Scale**

- Capacity sketch (compressed): ~600 B per IS event, ~1.5–2 KiB per
  `as.up.data.forward`. A site with 10 000 active devices at one uplink
  / 15 min generates ~30 events/s, ~3 GiB/day.
- For higher rates: drop high-volume payload fields in the Logstash
  filter (the `data` field of `as.up.data.forward` is most of the
  bytes), sample chatty events, or split forwarders by event-name
  regex (e.g. one for `^as\..+`, one for `^(ns|gs)\..+`).

**Observability**

A silent pipeline looks the same as "nothing is happening." At minimum:

- One freshness alert: page when no events have been indexed for the
  last few minutes ([§7.1](#71-an-example-alert-rule--ingest-lag) has an example rule).
- Stack monitoring of the storage / pipeline / Kibana tier itself,
  ideally writing to a different index from your event data so a
  storage outage doesn't take its own observability with it.

**Compliance**

TTS events contain personal data (`user_id`, `remote_ip`, `user_agent`,
device identifiers correlatable to physical hardware). Treat the index
as a personal-data store.

- Retention by class. Admin/security events (`application.*`,
  `gateway.*`, `user.*`, `oauth.*`, `*.api-key.*`, `*.collaborator.*`)
  warrant longer retention than traffic events. The single
  `RETENTION_DAYS` knob is the longer window; run a separate
  delete-by-query for shorter classes.
- Pseudonymise `remote_ip` (per-tenant salt + hash) if not required
  for forensics. The Logstash filter already drops `authentication`
  and `user_agent`.
- Subscribe to `user.delete` events and cascade to a delete-by-query
  on `user_id` to honour erasure requests.
- Match storage region to TTS region if regulation requires it.
- Note: purging an entity in TTS removes the entity, but events that
  mentioned it remain indexed. Define a cross-system deletion policy
  if you rely on this index for compliance reporting.

**Multi-tenancy**

Run one forwarder per TTS tenant with that tenant's API key, and set
`DATA_STREAM_NAMESPACE` so each tenant's events land in a distinct
data stream. Enforce per-tenant access at the Kibana layer.

**Lifecycle**

New TTS event names appear in nearly every release. The index template
uses `dynamic: true` so new fields appear automatically — but
dashboards pinned to a fixed list of names will silently miss them.
Review the TTS release notes on upgrade.

---

## 11. Appendix: useful event names

A non-exhaustive cheat-sheet for filter building. The authoritative list
lives in the [The Things Stack Documentation](https://www.thethingsindustries.com/docs/api/reference/grpc/events/).

| Component                   | Example event names                                                                                                                                                                                  | What they tell you            |
| --------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------- |
| Identity Server (no prefix) | `application.create`, `user.update`, `gateway.delete`, `oauth.authorize`, `oauth.token.exchange`, `oauth.user.login`, `account.user.login_failed`, `gateway.api-key.create`, `gateway.collaborator.update` | Lifecycle and auth audit.     |
| Network Server (`ns`)       | `ns.up.data.receive`, `ns.up.data.process`, `ns.up.join.receive`, `ns.up.join.accept.forward`, `ns.down.data.schedule.success`, `ns.mac.*`                                                          | LoRaWAN MAC layer.            |
| Application Server (`as`)   | `as.up.data.forward`, `as.up.data.drop`, `as.up.join.forward`, `as.down.data.forward`, `as.webhook.fail`                                                                                            | What the application sees.    |
| Gateway Server (`gs`)       | `gs.gateway.connect`, `gs.gateway.disconnect`, `gs.up.receive`, `gs.up.forward`, `gs.down.send`, `gs.status.receive`                                                                                 | Link health and traffic.      |
| Join Server (`js`)          | `js.join.accept`, `js.join.reject`                                                                                                                                                                   | Joins, including reasons.     |
| Device Claiming (`dcs`)     | `dcs.end_device.claim.success`, `dcs.gateway.claim.fail`                                                                                                                                             | Claim/unclaim flows.          |

Identity Server events have **no `is.` prefix** — the IS emits names
like `application.create`, `user.update`, `oauth.authorize` directly.

Use these with the parsed prefix fields, e.g.:

```
event_component : "as" and event_category : "up" and event_action : "drop"
```

---

## License

[Apache 2.0](LICENSE).
