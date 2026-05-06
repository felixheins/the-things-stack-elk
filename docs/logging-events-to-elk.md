---
title: Logging Events to the ELK Stack
description: Stream every event emitted by The Things Stack into Elasticsearch and explore them in Kibana.
weight: 50
---

The Things Stack exposes a streaming Events API that emits a structured JSON message for every internal event: gateway connect/disconnect, uplink received, downlink scheduled, join accept, application-layer forward, identity-server logins, OAuth token issuance, and many more. This guide walks through forwarding every such event into an ELK stack — Elasticsearch for storage, Logstash for parsing, and Kibana for search and dashboards — to give you a queryable audit and observability trail of network activity.

You can apply the configuration below to The Things Stack Cloud, Enterprise on-premises, or Open Source deployments. All the files you need are included inline.

##### Note:

This guide assumes basic familiarity with The Things Stack, the [`ttn-lw-cli`](https://www.thethingsindustries.com/docs/the-things-stack/interact/cli/) or Console, Docker, and the [Elastic Stack](https://www.elastic.co/elastic-stack). If you only want to forward events to a third-party service without operating your own database, consider using a [Webhook integration](https://www.thethingsindustries.com/docs/integrations/webhooks/) instead.

## Architecture

```
The Things Stack ──POST /api/v3/events──▶ forwarder ──TCP json──▶ Logstash ──bulk──▶ Elasticsearch ◀── Kibana
```

A small forwarder service opens a long-lived subscription to the [Events API](https://www.thethingsindustries.com/docs/api/reference/grpc/events/) and ships each event as one JSON line over TCP to Logstash. Logstash parses the canonical event timestamp, splits the dotted event name (e.g. `as.up.forward`) into component parts, promotes common entity identifiers to top-level fields, and writes to a time-based Elasticsearch data stream governed by an ILM policy. Kibana provides Discover, Lens, and dashboarding on top.

A dedicated forwarder is used because the Events API is a long-lived gRPC-Gateway stream rather than a poll-friendly REST endpoint, and because Logstash's HTTP inputs do not cleanly handle indefinite streaming responses.

## Prerequisites

- A The Things Stack deployment reachable over HTTPS.
- A user account on that deployment with rights to create API keys on the entities you want to observe.
- A host with Docker Engine 24 or later and the Compose plugin v2.
- At least 8 GiB of free RAM and 50 GiB of free disk for a single-node lab installation.
- Outbound HTTPS connectivity from the host to your The Things Stack hostname.

## Creating an API Key

The forwarder needs an API key with two distinct families of rights — one to discover entities to subscribe to, and one to receive events from those entities.

##### Note:

Having `RIGHT_APPLICATION_INFO` does **not** allow listing applications. Listing requires the user-level `RIGHT_USER_APPLICATIONS_LIST`, which is a separate right that must be granted explicitly. The same applies to gateways, organizations, and OAuth clients. Admin status (`is_admin: true`) does not bypass this check.

Open the Console at `https://<your-tts-host>/console/user-api-keys/add`, name the key `tts-elk-forwarder`, and grant the rights from both columns below:

| List rights (entity discovery)         | Info / traffic rights (event visibility)         |
|----------------------------------------|--------------------------------------------------|
| `RIGHT_USER_APPLICATIONS_LIST`         | `RIGHT_APPLICATION_INFO`, `RIGHT_APPLICATION_TRAFFIC_READ` |
| `RIGHT_USER_GATEWAYS_LIST`             | `RIGHT_GATEWAY_INFO`, `RIGHT_GATEWAY_TRAFFIC_READ`, `RIGHT_GATEWAY_STATUS_READ` |
| `RIGHT_USER_ORGANIZATIONS_LIST`        | `RIGHT_ORGANIZATION_INFO`                        |
| `RIGHT_USER_CLIENTS_LIST` (optional)   | `RIGHT_CLIENT_INFO` (optional)                   |
|                                        | `RIGHT_USER_INFO`                                |

##### Warning:

Save the bearer token immediately. The Things Stack displays the value only once and there is no way to retrieve it later.

If your API key is scoped (e.g. a collaborator key on a single application), you can still capture events for that entity — see [Subscribing Without List Rights](#subscribing-without-list-rights).

## Project Layout

Create a working directory with the layout below. The full content of each file is given in the following sections.

```
tts-elk/
├── .env
├── docker-compose.yml
├── elasticsearch/
│   └── setup.sh
├── forwarder/
│   ├── Dockerfile
│   ├── forwarder.py
│   └── requirements.txt
└── logstash/
    ├── config/
    │   └── logstash.yml
    └── pipeline/
        └── tts-events.conf
```

## Configuration

Create `.env` in the working directory with the values below. Replace `TTS_HOST` with your deployment's hostname and `TTS_API_KEY` with the bearer token from the previous step. Generate the Kibana encryption key with `openssl rand -hex 32`.

```
# The Things Stack
TTS_HOST=<tenant>.eu1.cloud.thethings.industries
TTS_API_KEY=NNSXS.replace-me
TTS_INSECURE=false

SUBSCRIBE_KINDS=applications,gateways,organizations,users,clients,end_devices
EVENT_NAMES_REGEX=/.+/
REFRESH_INTERVAL=300
STATIC_IDENTIFIERS=

# Elastic Stack
STACK_VERSION=8.13.4
ELASTIC_PASSWORD=change-me-elastic
KIBANA_PASSWORD=change-me-kibana
KIBANA_ENCRYPTION_KEY=replace_with_openssl_rand_hex_32

ES_JAVA_OPTS=-Xms2g -Xmx2g
LS_JAVA_OPTS=-Xms512m -Xmx512m
ES_MEM_LIMIT=4g
KB_MEM_LIMIT=1g
LS_MEM_LIMIT=1g

KIBANA_PORT=5601
ELASTICSEARCH_PORT=9200
RETENTION_DAYS=90

# Logstash → Elasticsearch endpoint. Defaults to the in-stack ES; override
# to target Elastic Cloud / a managed cluster (see Going to Production).
ES_HOSTS=http://elasticsearch:9200
ES_USER=elastic
ES_PASSWORD=${ELASTIC_PASSWORD}
DATA_STREAM_NAMESPACE=default
```

##### Note:

`SUBSCRIBE_KINDS` includes `end_devices` so device-scoped API keys (no application-list rights) can stream traffic via `STATIC_IDENTIFIERS`. For tenant-wide keys it is functionally redundant — TTS hierarchical identifier matching means events for a device are also delivered through the parent application's subscription, and the forwarder uses the The Things Stack `unique_id` as the Elasticsearch document `_id` so duplicates collapse to a single document.

##### Warning:

Do not commit the `.env` file. Add it to `.gitignore` and inject `TTS_API_KEY` and the database passwords from a secrets manager in any production deployment. See [Going to Production](#going-to-production).

## Setting Up the ELK Stack

Save the following as `docker-compose.yml`. It runs Elasticsearch, Kibana, Logstash, a one-shot setup container, and the forwarder on a single host. The defaults are tuned for a lab — security is enabled, but in-cluster traffic uses HTTP rather than TLS to keep the first-run experience simple. See [Going to Production](#going-to-production) for hardening.

```
services:

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:${STACK_VERSION}
    environment:
      - node.name=es01
      - cluster.name=tts-events
      - discovery.type=single-node
      - ELASTIC_PASSWORD=${ELASTIC_PASSWORD}
      - bootstrap.memory_lock=true
      - xpack.security.enabled=true
      - xpack.security.http.ssl.enabled=false
      - xpack.security.transport.ssl.enabled=false
      - xpack.license.self_generated.type=basic
      - ES_JAVA_OPTS=${ES_JAVA_OPTS}
    ulimits:
      memlock: { soft: -1, hard: -1 }
    mem_limit: ${ES_MEM_LIMIT}
    volumes:
      - esdata:/usr/share/elasticsearch/data
    ports:
      - "${ELASTICSEARCH_PORT}:9200"
    networks: [ elk ]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS -u elastic:${ELASTIC_PASSWORD} http://localhost:9200/_cluster/health | grep -E '\"status\":\"(green|yellow)\"'"]
      interval: 10s
      timeout: 5s
      retries: 60

  setup:
    image: docker.elastic.co/elasticsearch/elasticsearch:${STACK_VERSION}
    depends_on:
      elasticsearch: { condition: service_healthy }
    environment:
      - ELASTIC_PASSWORD=${ELASTIC_PASSWORD}
      - KIBANA_PASSWORD=${KIBANA_PASSWORD}
      - RETENTION_DAYS=${RETENTION_DAYS}
    volumes:
      - ./elasticsearch:/setup:ro
    networks: [ elk ]
    entrypoint: [ "/bin/bash", "/setup/setup.sh" ]
    restart: "no"

  kibana:
    image: docker.elastic.co/kibana/kibana:${STACK_VERSION}
    depends_on:
      elasticsearch: { condition: service_healthy }
      setup: { condition: service_completed_successfully }
    environment:
      - ELASTICSEARCH_HOSTS=http://elasticsearch:9200
      - ELASTICSEARCH_USERNAME=kibana_system
      - ELASTICSEARCH_PASSWORD=${KIBANA_PASSWORD}
      - XPACK_SECURITY_ENCRYPTIONKEY=${KIBANA_ENCRYPTION_KEY}
      - XPACK_ENCRYPTEDSAVEDOBJECTS_ENCRYPTIONKEY=${KIBANA_ENCRYPTION_KEY}
      - XPACK_REPORTING_ENCRYPTIONKEY=${KIBANA_ENCRYPTION_KEY}
    mem_limit: ${KB_MEM_LIMIT}
    ports:
      - "${KIBANA_PORT}:5601"
    networks: [ elk ]

  logstash:
    image: docker.elastic.co/logstash/logstash:${STACK_VERSION}
    depends_on:
      elasticsearch: { condition: service_healthy }
      setup: { condition: service_completed_successfully }
    environment:
      - LS_JAVA_OPTS=${LS_JAVA_OPTS}
      - ES_HOSTS=${ES_HOSTS:-http://elasticsearch:9200}
      - ES_USER=${ES_USER:-elastic}
      - ES_PASSWORD=${ES_PASSWORD:-${ELASTIC_PASSWORD}}
      - DATA_STREAM_NAMESPACE=${DATA_STREAM_NAMESPACE:-default}
    mem_limit: ${LS_MEM_LIMIT}
    volumes:
      - ./logstash/config/logstash.yml:/usr/share/logstash/config/logstash.yml:ro
      - ./logstash/pipeline:/usr/share/logstash/pipeline:ro
    networks: [ elk ]
    healthcheck:
      test: ["CMD-SHELL", "curl -fsS http://localhost:9600 >/dev/null"]
      interval: 15s
      timeout: 5s
      retries: 20

  forwarder:
    build: ./forwarder
    depends_on:
      logstash: { condition: service_healthy }
    environment:
      - TTS_HOST=${TTS_HOST}
      - TTS_API_KEY=${TTS_API_KEY}
      - TTS_INSECURE=${TTS_INSECURE}
      - SUBSCRIBE_KINDS=${SUBSCRIBE_KINDS}
      - EVENT_NAMES_REGEX=${EVENT_NAMES_REGEX}
      - REFRESH_INTERVAL=${REFRESH_INTERVAL}
      - STATIC_IDENTIFIERS=${STATIC_IDENTIFIERS}
      - LOGSTASH_HOST=logstash
      - LOGSTASH_PORT=5044
      - LOG_LEVEL=INFO
    restart: unless-stopped
    networks: [ elk ]
    healthcheck:
      test: ["CMD-SHELL", "test -f /tmp/forwarder-alive && [ $(($(date +%s) - $(stat -c %Y /tmp/forwarder-alive))) -lt 90 ]"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 60s

volumes:
  esdata:

networks:
  elk:
    driver: bridge
```

## Configuring Elasticsearch

The setup container installs an [Index Lifecycle Management](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-lifecycle-management.html) policy and the index templates that govern how events are stored. Save the following as `elasticsearch/setup.sh` and make it executable (`chmod +x elasticsearch/setup.sh`):

```
#!/usr/bin/env bash
set -euo pipefail

ES="http://elasticsearch:9200"
AUTH="elastic:${ELASTIC_PASSWORD}"

until curl -fsS -u "$AUTH" "$ES/_cluster/health?wait_for_status=yellow&timeout=30s" >/dev/null; do
  echo "waiting for elasticsearch..."; sleep 3
done

curl -fsS -u "$AUTH" -X POST "$ES/_security/user/kibana_system/_password" \
  -H 'Content-Type: application/json' \
  -d "{\"password\":\"${KIBANA_PASSWORD}\"}" >/dev/null

curl -fsS -u "$AUTH" -X PUT "$ES/_ilm/policy/tts-events-ilm" \
  -H 'Content-Type: application/json' \
  -d @- <<EOF >/dev/null
{
  "policy": { "phases": {
    "hot":    { "actions": { "rollover": { "max_age": "1d", "max_primary_shard_size": "20gb" } } },
    "warm":   { "min_age": "2d",  "actions": { "forcemerge": { "max_num_segments": 1 } } },
    "delete": { "min_age": "${RETENTION_DAYS}d", "actions": { "delete": {} } }
  } }
}
EOF

curl -fsS -u "$AUTH" -X PUT "$ES/_component_template/tts-events-mappings" \
  -H 'Content-Type: application/json' \
  -d @- <<'EOF' >/dev/null
{ "template": {
  "settings": {
    "index.lifecycle.name": "tts-events-ilm",
    "index.codec": "best_compression",
    "index.mapping.total_fields.limit": 5000
  },
  "mappings": { "dynamic": "true", "properties": {
    "@timestamp":      { "type": "date" },
    "time":            { "type": "date" },
    "name":            { "type": "keyword" },
    "event_component": { "type": "keyword" },
    "event_category":  { "type": "keyword" },
    "event_action":    { "type": "keyword" },
    "unique_id":       { "type": "keyword" },
    "origin":          { "type": "keyword" },
    "remote_ip":       { "type": "ip" },
    "application_id":  { "type": "keyword" },
    "gateway_id":      { "type": "keyword" },
    "gateway_eui":     { "type": "keyword" },
    "device_id":       { "type": "keyword" },
    "dev_eui":         { "type": "keyword" },
    "join_eui":        { "type": "keyword" },
    "organization_id": { "type": "keyword" },
    "user_id":         { "type": "keyword" },
    "client_id":       { "type": "keyword" },
    "correlation_ids": { "type": "keyword" },
    "context":         { "type": "object", "enabled": false },
    "visibility":      { "type": "object", "enabled": false }
  } }
} }
EOF

curl -fsS -u "$AUTH" -X PUT "$ES/_index_template/tts-events" \
  -H 'Content-Type: application/json' \
  -d @- <<'EOF' >/dev/null
{
  "index_patterns": ["logs-tts.events-*"],
  "data_stream": {},
  "composed_of": ["tts-events-mappings"],
  "priority": 500
}
EOF

echo "setup complete."
```

The ILM policy rolls over backing indices daily or once a primary shard reaches 20 GiB, force-merges them after two days, and deletes them after `RETENTION_DAYS`. The component template disables indexing on `context` and `visibility` (whose schemas are open-ended) to avoid mapping explosions, while still storing them.

## Configuring Logstash

Save the following as `logstash/config/logstash.yml`:

```
http.host: 0.0.0.0
xpack.monitoring.enabled: false
pipeline.ecs_compatibility: v8
log.level: info
```

##### Note:

`pipeline.ecs_compatibility: v8` is required by Logstash 8.x when writing to a data stream. Disabling it causes the Elasticsearch output to fail to register.

Save the following as `logstash/pipeline/tts-events.conf`:

```
input {
  tcp {
    port  => 5044
    codec => json_lines
  }
}

filter {
  if [time] {
    date {
      match  => ["time", "ISO8601"]
      target => "@timestamp"
    }
  }

  if [name] {
    dissect {
      mapping => { "name" => "%{event_component}.%{event_category}.%{event_action}" }
    }
  }

  if [identifiers] {
    ruby {
      code => '
        ids = event.get("identifiers") || []
        ids.each do |i|
          if (a = i["application_ids"]); event.set("application_id", a["application_id"]); end
          if (g = i["gateway_ids"])
            event.set("gateway_id",  g["gateway_id"])
            event.set("gateway_eui", g["eui"]) if g["eui"]
          end
          if (d = i["device_ids"])
            event.set("device_id", d["device_id"])
            event.set("dev_eui",   d["dev_eui"])  if d["dev_eui"]
            event.set("join_eui",  d["join_eui"]) if d["join_eui"]
            event.set("application_id", d["application_ids"]["application_id"]) if d["application_ids"]
          end
          if (o = i["organization_ids"]); event.set("organization_id", o["organization_id"]); end
          if (u = i["user_ids"]);         event.set("user_id",         u["user_id"]);         end
          if (c = i["client_ids"]);       event.set("client_id",       c["client_id"]);       end
        end
      '
    }
  }

  mutate { remove_field => ["authentication", "user_agent"] }
}

output {
  elasticsearch {
    hosts    => ["${ES_HOSTS:http://elasticsearch:9200}"]
    user     => "${ES_USER:elastic}"
    password => "${ES_PASSWORD}"
    data_stream           => "true"
    data_stream_type      => "logs"
    data_stream_dataset   => "tts.events"
    data_stream_namespace => "${DATA_STREAM_NAMESPACE:default}"
    document_id => "%{unique_id}"
  }
}
```

The pipeline uses the The Things Stack-supplied `unique_id` (a ULID) as the Elasticsearch document `_id`. This makes any redelivery during a forwarder reconnect produce an idempotent overwrite rather than a duplicate.

## Building the Forwarder

The forwarder is a small asynchronous Python service. It opens one streaming subscription per identifier kind because the Events API does not accept mixed identifier kinds in a single request; subscriptions are merged into a shared TCP sink that writes to Logstash.

Save the following as `forwarder/Dockerfile`:

```
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY forwarder.py .
USER nobody
ENTRYPOINT ["python", "-u", "forwarder.py"]
```

Save the following as `forwarder/requirements.txt`:

```
httpx==0.27.2
```

Save the following as `forwarder/forwarder.py`:

```
import asyncio, json, logging, os, pathlib, signal, sys, time
from typing import Any, Iterable
import httpx

TTS_HOST     = os.environ["TTS_HOST"].rstrip("/").removeprefix("https://").removeprefix("http://")
TTS_API_KEY  = os.environ["TTS_API_KEY"]
TTS_INSECURE = os.environ.get("TTS_INSECURE", "false").lower() == "true"
LS_HOST      = os.environ.get("LOGSTASH_HOST", "logstash")
LS_PORT      = int(os.environ.get("LOGSTASH_PORT", "5044"))
REFRESH      = int(os.environ.get("REFRESH_INTERVAL", "300"))
KINDS        = tuple(k.strip() for k in os.environ.get(
    "SUBSCRIBE_KINDS", "applications,gateways,organizations,users,clients,end_devices"
).split(",") if k.strip())
NAMES_REGEX  = os.environ.get("EVENT_NAMES_REGEX", "/.+/")
LOG_LEVEL    = os.environ.get("LOG_LEVEL", "INFO").upper()
HEARTBEAT    = pathlib.Path("/tmp/forwarder-alive")

STATIC_IDENTIFIERS: dict[str, list[dict]] = {}
raw = os.environ.get("STATIC_IDENTIFIERS", "").strip()
if raw:
    STATIC_IDENTIFIERS = json.loads(raw)

logging.basicConfig(level=LOG_LEVEL, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("forwarder")

BASE    = f"https://{TTS_HOST}/api/v3"
HEADERS = {"Authorization": f"Bearer {TTS_API_KEY}", "User-Agent": "tts-elk-forwarder/1.0"}
META = {
  "applications":  {"path": "applications",  "key": "applications",  "field": "application_ids"},
  "gateways":      {"path": "gateways",      "key": "gateways",      "field": "gateway_ids"},
  "organizations": {"path": "organizations", "key": "organizations", "field": "organization_ids"},
  "users":         {"path": "users",         "key": "users",         "field": "user_ids"},
  "clients":       {"path": "clients",       "key": "clients",       "field": "client_ids"},
  "end_devices":   {"path": None,            "key": "end_devices",   "field": "end_device_ids"},
}

async def list_entities(client: httpx.AsyncClient, kind: str) -> list[dict]:
    if kind == "end_devices":
        # End devices have no top-level list endpoint — walk applications.
        out = []
        for app in await list_entities(client, "applications"):
            app_id = (app.get("ids") or {}).get("application_id")
            if not app_id: continue
            page = 1
            while True:
                r = await client.get(f"{BASE}/applications/{app_id}/devices",
                                     headers=HEADERS, params={"limit": 1000, "page": page}, timeout=30)
                if r.status_code == 403: break
                r.raise_for_status()
                items = r.json().get("end_devices") or []
                if not items: break
                out.extend(items); page += 1
        return out
    out, page = [], 1
    while True:
        r = await client.get(f"{BASE}/{META[kind]['path']}", headers=HEADERS,
                             params={"limit": 1000, "page": page}, timeout=30)
        if r.status_code == 403:
            log.info("api key cannot list %s — skipping", kind); return []
        r.raise_for_status()
        items = r.json().get(META[kind]["key"], []) or []
        if not items: return out
        out.extend(items); page += 1

async def heartbeat(stop):
    """Touch a file every 30s so the docker-compose healthcheck has a signal."""
    while not stop.is_set():
        try: HEARTBEAT.write_text(str(int(time.time())))
        except OSError: pass
        try: await asyncio.wait_for(stop.wait(), timeout=30)
        except asyncio.TimeoutError: pass

class Sink:
    def __init__(self, host, port):
        self.host, self.port = host, port
        self.w: asyncio.StreamWriter | None = None
        self.lock = asyncio.Lock()
    async def _connect(self):
        delay = 1.0
        while True:
            try:
                _, self.w = await asyncio.open_connection(self.host, self.port)
                log.info("connected to logstash %s:%d", self.host, self.port); return
            except OSError as e:
                log.warning("logstash connect failed (%s); retry in %.1fs", e, delay)
                await asyncio.sleep(delay); delay = min(delay * 2, 30)
    async def write(self, line: bytes):
        async with self.lock:
            for _ in range(5):
                if self.w is None: await self._connect()
                try:
                    self.w.write(line); await self.w.drain(); return
                except (OSError, ConnectionError) as e:
                    log.warning("logstash write failed (%s)", e)
                    try: self.w.close(); await self.w.wait_closed()
                    except Exception: pass
                    self.w = None
            log.error("dropped event after 5 failed sends")

async def iter_events(r: httpx.Response):
    """Yield events from a TTS streaming response (NDJSON or SSE-framed)."""
    buf: list[str] = []
    async for raw in r.aiter_lines():
        if raw == "":
            if buf:
                payload = "".join(buf); buf.clear()
                try: env = json.loads(payload)
                except json.JSONDecodeError: continue
                yield env.get("result", env)
            continue
        if raw.startswith(":"): continue
        if raw.startswith("data:"):
            chunk = raw[5:]
            buf.append(chunk[1:] if chunk.startswith(" ") else chunk); continue
        if raw.lstrip().startswith("{"):
            try: env = json.loads(raw)
            except json.JSONDecodeError: buf.append(raw); continue
            yield env.get("result", env)

async def subscribe(client, kind, sink, stop):
    backoff = 1.0
    while not stop.is_set():
        try:
            if kind in STATIC_IDENTIFIERS:
                ids = [{META[kind]["field"]: i} for i in STATIC_IDENTIFIERS[kind]]
            else:
                ids = [{META[kind]["field"]: e["ids"]} for e in await list_entities(client, kind) if "ids" in e]
            if not ids:
                log.info("no %s visible — sleeping", kind); await asyncio.sleep(REFRESH); continue
            log.info("[%s] subscribing to %d entities", kind, len(ids))
            body = {"identifiers": ids, "tail": 0, "names": [NAMES_REGEX]}
            async with client.stream("POST", f"{BASE}/events",
                headers={**HEADERS, "Accept": "text/event-stream"}, json=body,
                timeout=httpx.Timeout(connect=10, read=None, write=10, pool=10)) as r:
                r.raise_for_status()
                deadline = asyncio.get_running_loop().time() + REFRESH
                async for evt in iter_events(r):
                    evt["_subscription_kind"] = kind
                    await sink.write((json.dumps(evt, separators=(",", ":")) + "\n").encode())
                    if asyncio.get_running_loop().time() >= deadline: break
                    if stop.is_set(): return
            backoff = 1.0
        except (httpx.HTTPError, OSError) as e:
            log.warning("[%s] %s — reconnect in %.1fs", kind, e, backoff)
            await asyncio.sleep(backoff); backoff = min(backoff * 2, 60)

async def main():
    stop = asyncio.Event()
    for s in (signal.SIGINT, signal.SIGTERM):
        asyncio.get_running_loop().add_signal_handler(s, stop.set)
    sink = Sink(LS_HOST, LS_PORT)
    async with httpx.AsyncClient(verify=not TTS_INSECURE) as client:
        ws = [asyncio.create_task(subscribe(client, k, sink, stop)) for k in KINDS if k in META]
        ws.append(asyncio.create_task(heartbeat(stop)))
        await stop.wait()
        for w in ws: w.cancel()
        await asyncio.gather(*ws, return_exceptions=True)

if __name__ == "__main__":
    try: asyncio.run(main())
    except KeyboardInterrupt: sys.exit(0)
```

## Running the Stack

From the working directory:

```
docker compose up -d --build
```

The first start-up performs the following sequence:

1. Pulls Elasticsearch, Kibana, and Logstash images of the version pinned in `.env`.
2. Builds the forwarder image.
3. Starts Elasticsearch and waits until cluster health reports `yellow` or `green`.
4. Runs the one-shot `setup` container, which sets the `kibana_system` password and installs the ILM policy and index templates.
5. Starts Kibana, Logstash, and finally the forwarder.

Once all services are healthy, Kibana is available on `http://localhost:5601` and Elasticsearch on `http://localhost:9200`. Log in as `elastic` with the password from `.env`.

##### Note:

The forwarder logs `subscribing to N entities` per identifier kind once it has connected. If you see `403 Forbidden` from `/applications`, `/gateways`, etc., your API key is missing the corresponding `RIGHT_USER_*_LIST` right — see [Subscribing Without List Rights](#subscribing-without-list-rights).

## Verifying Events Are Indexed

Confirm that documents are landing in the data stream:

```
curl -fsS -u elastic:<password> http://localhost:9200/logs-tts.events-default/_count
```

The count should grow over time as The Things Stack emits events. Inspect the most recent document:

```
curl -fsS -u elastic:<password> \
  "http://localhost:9200/logs-tts.events-default/_search?size=1&sort=@timestamp:desc&pretty"
```

To browse in Kibana, open **Stack Management → Data Views → Create data view**, set the index pattern to `logs-tts.events-*` and the timestamp field to `@timestamp`. Then open **Discover** and select the new data view.

Useful KQL queries to try in the search bar:

```
event_component : "gs" and event_action : ("connect" or "disconnect")
event_category : "join"
event_action : "drop"
event_component : "is" and event_category : "oauth"
event_component : "as" and event_category : "up"
```

### An Example Alert Rule — Ingest Lag

The single highest-signal alarm is "no events have been indexed in the last few minutes": it catches forwarder hangs, Events API outages, and Logstash backpressure with one rule. Create it via the Kibana Alerting API:

```
curl -fsS -u elastic:<password> \
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

The rule fires when fewer than 1 document is indexed in the last 5 minutes. Wire it to a connector (Slack, email, PagerDuty) by populating the empty `"actions"` array; see the [Kibana Alerting documentation](https://www.elastic.co/guide/en/kibana/current/create-and-manage-rules.html) for the connector schema.

## Subscribing Without List Rights

If your API key has `*_INFO` and `*_TRAFFIC_READ` rights but not the user-level `*_LIST` rights, the forwarder cannot enumerate entities. Set `STATIC_IDENTIFIERS` in `.env` to a JSON object keyed by kind:

```
STATIC_IDENTIFIERS={"gateways":[{"gateway_id":"my-gateway"}],"applications":[{"application_id":"my-app"}]}
SUBSCRIBE_KINDS=gateways,applications
```

The forwarder will skip discovery entirely and subscribe directly to the supplied identifiers.

For end devices use the [`EndDeviceIdentifiers`](https://www.thethingsindustries.com/docs/api/reference/grpc/end_device/) shape — the device ID alongside its parent application:

```
STATIC_IDENTIFIERS={"end_devices":[{"device_id":"my-dev","application_ids":{"application_id":"my-app"}}]}
SUBSCRIBE_KINDS=end_devices
```

This is the case where `end_devices` carries its weight — typically a key issued to a per-device collaborator that has neither application-list nor application-info rights.

## Targeting an External Elasticsearch

The Logstash output is fully environment-driven, so pointing the pipeline at Elastic Cloud or a managed cluster needs no code change. Override the connection variables in `.env`:

```
ES_HOSTS=https://my-deployment.es.eu-west-1.aws.found.io:9243
ES_USER=elastic
ES_PASSWORD=your-cloud-password
```

Then start only the components you need (`docker compose up -d logstash forwarder`); the bundled `elasticsearch` and `kibana` services become unused. The setup script in `elasticsearch/setup.sh` runs the curl calls that install the ILM policy and the index template — you can run them by hand against any cluster by setting `ES` and `AUTH` to the external endpoint, or skip them and rely on auto-created mappings.

For self-managed clusters with a private CA, mount the CA file into the Logstash container and add `cacert => "/path/to/ca.pem"` next to the `hosts` line in the Elasticsearch output.

## Going to Production

The defaults above are tuned for a lab on a single host. Before running this against real workloads, address the following:

**Security.** Generate certificates with `bin/elasticsearch-certutil` and enable TLS on the Elasticsearch HTTP and transport interfaces. Do not expose Kibana directly to the Internet — front it with an SSO-aware reverse proxy. Replace the `elastic` superuser used by Logstash with a dedicated role that only has `create_doc`/`auto_configure` on `logs-tts.events-*`. Inject `TTS_API_KEY` and database passwords from a secrets manager rather than `.env`.

**Reliability.** Replace the forwarder → Logstash TCP socket with [Apache Kafka](https://kafka.apache.org/) or [Redpanda](https://redpanda.com/) so a Logstash outage does not cause event loss. Run a 3-node Elasticsearch cluster with dedicated master and data roles, and configure [Snapshot Lifecycle Management](https://www.elastic.co/guide/en/elasticsearch/reference/current/snapshot-lifecycle-management.html) for backups. The forwarder is stateless, so two replicas can run active-active behind the `unique_id` deduplication; this doubles API quota usage but eliminates the single point of failure.

**Scale.** Approximate sizes after compression: ~600 B per Identity Server event and 1.5–2 KiB per `as.up.forward` (which carries the full `ApplicationUp` payload). A network with 10 000 active devices at one uplink per 15 minutes generates ~30 events/s across all components, or ~3 GiB/day. For higher volumes, drop the decoded payload from `as.up.forward` events in the Logstash filter, sample chatty events, or split forwarders by event-name regex (e.g. one for `^as\..+`, one for `^(ns|gs)\..+`).

**Observability.** Run [Metricbeat or Elastic Agent](https://www.elastic.co/guide/en/beats/metricbeat/current/metricbeat-module-elasticsearch.html) against the cluster. Add a Kibana threshold rule that pages on `max(now() - @timestamp) > 5m`, which catches forwarder hangs, API outages, and Logstash backpressure with a single signal.

**Compliance.** Events contain personal data (`user_id`, `remote_ip`, `user_agent`, device identifiers). Set retention by data class, with admin/security events kept longer than traffic events. Pseudonymise `remote_ip` with a per-tenant salt if it is not required for forensics. Use Kibana Spaces and document-level security to partition access. Consume `is.user.delete` events to honour data-subject erasure requests.

**Multi-tenancy.** Run one forwarder per The Things Stack tenant with its own API key, and set a distinct `data_stream_namespace` per tenant in the Logstash output (e.g. `tenant-acme` produces `logs-tts.events-tenant-acme`). Use Kibana Spaces and role-based DLS to keep tenants isolated.

##### Warning:

Purging an entity in The Things Stack [permanently deletes its identifiers](https://www.thethingsindustries.com/docs/concepts/advanced/purge/), but events that mentioned that entity remain in Elasticsearch. Define a deletion policy that aligns the two systems if you rely on this index for compliance reporting.

## Useful Event Names

A non-exhaustive cheat-sheet for filter building. The authoritative list lives in [`pkg/events`](https://github.com/TheThingsNetwork/lorawan-stack/tree/main/pkg/events) and per-component event registrations in The Things Stack source.

| Component               | Examples                                                                       | Meaning                              |
|-------------------------|--------------------------------------------------------------------------------|--------------------------------------|
| Identity Server (`is`)  | `is.user.create`, `is.application.create`, `is.oauth.access_token.create`      | Lifecycle and auth audit             |
| Network Server (`ns`)   | `ns.up.receive`, `ns.up.join.receive`, `ns.down.schedule.success`, `ns.mac.*`  | LoRaWAN MAC layer                    |
| Application Server (`as`) | `as.up.forward`, `as.up.drop`, `as.down.data.forward`                        | What the application receives        |
| Gateway Server (`gs`)   | `gs.gateway.connect`, `gs.gateway.disconnect`, `gs.up.receive`, `gs.down.send` | Link health and traffic              |
| Join Server (`js`)      | `js.join.accept`, `js.join.reject`                                             | Joins, including reasons             |
| Packet Broker (`pba`)   | `pba.uplink.forward`, `pba.downlink.receive`                                   | Roaming traffic                      |

## Related Resources

- [Events API reference](https://www.thethingsindustries.com/docs/api/reference/grpc/events/)
- [Webhooks integration](https://www.thethingsindustries.com/docs/integrations/webhooks/) — for forwarding events to external HTTP endpoints rather than collecting them centrally.
- [The `ttn-lw-cli` reference](https://www.thethingsindustries.com/docs/the-things-stack/interact/cli/)
- [Elastic Common Schema](https://www.elastic.co/guide/en/ecs/current/index.html)
- [Index Lifecycle Management](https://www.elastic.co/guide/en/elasticsearch/reference/current/index-lifecycle-management.html)
- [Elastic Cloud on Kubernetes](https://www.elastic.co/guide/en/cloud-on-k8s/current/index.html)
