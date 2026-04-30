"""
TTS → Logstash events forwarder.

Subscribes to The Things Stack's streaming Events API for every entity the
configured API key can see, parses the Server-Sent Events response, and
ships each event as one JSON line to a Logstash TCP input.

The TTS Events API only accepts identifiers of a *single kind* per request
(applications, gateways, organizations, etc.), so this forwarder runs one
asyncio task per identifier kind and merges them into a shared sink.

Configuration is via environment variables — see .env.example at the
repo root.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
from typing import Any, Iterable

import httpx

# ---------- configuration ---------------------------------------------------

TTS_HOST = os.environ["TTS_HOST"].rstrip("/").removeprefix("https://").removeprefix("http://")
TTS_API_KEY = os.environ["TTS_API_KEY"]
TTS_INSECURE = os.environ.get("TTS_INSECURE", "false").lower() == "true"

LS_HOST = os.environ.get("LOGSTASH_HOST", "logstash")
LS_PORT = int(os.environ.get("LOGSTASH_PORT", "5044"))

# How often to re-list entities and reopen streams so newly-created
# applications/gateways/etc. start being captured. Cheap on the API side
# (a handful of paginated GETs).
REFRESH_INTERVAL = int(os.environ.get("REFRESH_INTERVAL", "300"))

# Comma-separated list of identifier kinds to subscribe to. Useful for
# scoping in tests, or for splitting load across multiple forwarders.
KINDS = tuple(
    k.strip() for k in os.environ.get(
        "SUBSCRIBE_KINDS",
        "applications,gateways,organizations,users,clients",
    ).split(",") if k.strip()
)

# Optional regex filter on event names ('.+' = everything, the default).
EVENT_NAMES_REGEX = os.environ.get("EVENT_NAMES_REGEX", "/.+/")

LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()

# Optional static identifier override. When set, the forwarder skips entity
# discovery entirely and subscribes to exactly this set. Useful when the
# API key cannot list entities (no `RIGHT_USER_*_LIST` rights) but can
# subscribe to events on specific entities it already knows about.
#
# Format: a JSON object keyed by kind, each value an array of EntityIdentifiers
# of that kind:
#   STATIC_IDENTIFIERS='{
#     "gateways":     [{"gateway_id": "my-gateway"}],
#     "applications": [{"application_id": "my-app"}]
#   }'
STATIC_IDENTIFIERS_RAW = os.environ.get("STATIC_IDENTIFIERS", "").strip()
STATIC_IDENTIFIERS: dict[str, list[dict]] = {}
if STATIC_IDENTIFIERS_RAW:
    try:
        STATIC_IDENTIFIERS = json.loads(STATIC_IDENTIFIERS_RAW)
        if not isinstance(STATIC_IDENTIFIERS, dict):
            raise ValueError("STATIC_IDENTIFIERS must be a JSON object")
    except (ValueError, json.JSONDecodeError) as e:
        print(f"FATAL: invalid STATIC_IDENTIFIERS: {e}", file=sys.stderr)
        sys.exit(2)

logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("forwarder")

# ---------- TTS API helpers -------------------------------------------------

BASE = f"https://{TTS_HOST}/api/v3"
HEADERS = {
    "Authorization": f"Bearer {TTS_API_KEY}",
    "User-Agent": "tts-elk-forwarder/1.0",
}

# Per-kind metadata: list endpoint, response key, and how to project the
# listed object into an EntityIdentifiers oneof.
KIND_META: dict[str, dict[str, Any]] = {
    "applications": {
        "list_path": "applications",
        "list_key": "applications",
        "ids_field": "application_ids",
    },
    "gateways": {
        "list_path": "gateways",
        "list_key": "gateways",
        "ids_field": "gateway_ids",
    },
    "organizations": {
        "list_path": "organizations",
        "list_key": "organizations",
        "ids_field": "organization_ids",
    },
    "users": {
        "list_path": "users",
        "list_key": "users",
        "ids_field": "user_ids",
    },
    "clients": {
        "list_path": "clients",
        "list_key": "clients",
        "ids_field": "client_ids",
    },
}


async def list_entities(client: httpx.AsyncClient, kind: str) -> list[dict]:
    """Page through a TTS list endpoint for the given entity kind."""
    meta = KIND_META[kind]
    out: list[dict] = []
    page = 1
    while True:
        r = await client.get(
            f"{BASE}/{meta['list_path']}",
            headers=HEADERS,
            params={"limit": 1000, "page": page},
            timeout=30,
        )
        if r.status_code == 403:
            log.info("api key has no rights to list %s — skipping", kind)
            return []
        r.raise_for_status()
        items = r.json().get(meta["list_key"], []) or []
        if not items:
            return out
        out.extend(items)
        page += 1


def to_identifiers(kind: str, entities: Iterable[dict]) -> list[dict]:
    """Convert a list of entities into the EntityIdentifiers oneof shape."""
    field = KIND_META[kind]["ids_field"]
    return [{field: e["ids"]} for e in entities if "ids" in e]


# ---------- Logstash sink ---------------------------------------------------


class LogstashSink:
    """Async, lock-protected, line-buffered TCP sink with reconnect."""

    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self._writer: asyncio.StreamWriter | None = None
        self._lock = asyncio.Lock()

    async def _connect(self) -> None:
        delay = 1.0
        while True:
            try:
                _, writer = await asyncio.open_connection(self.host, self.port)
                self._writer = writer
                log.info("connected to logstash %s:%d", self.host, self.port)
                return
            except OSError as e:
                log.warning("logstash connect failed (%s); retry in %.1fs", e, delay)
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)

    async def write(self, line: bytes) -> None:
        async with self._lock:
            for _ in range(5):
                if self._writer is None:
                    await self._connect()
                try:
                    assert self._writer is not None
                    self._writer.write(line)
                    await self._writer.drain()
                    return
                except (OSError, ConnectionError) as e:
                    log.warning("logstash write failed (%s); reconnecting", e)
                    try:
                        self._writer.close()
                        await self._writer.wait_closed()
                    except Exception:
                        pass
                    self._writer = None
            log.error("dropped event after 5 failed sends")

    async def aclose(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass


# ---------- SSE parser ------------------------------------------------------


async def iter_stream_events(response: httpx.Response):
    """Yield decoded events from a TTS streaming response.

    The Events API advertises ``Content-Type: text/event-stream`` but the
    on-the-wire format is plain newline-delimited JSON, one
    ``{"result": <event>}`` envelope per line with blank separators —
    the standard gRPC-Gateway streaming framing. We accept either:

    * raw NDJSON lines starting with ``{`` (current TTS behaviour), and
    * proper SSE ``data:`` lines (defensive — in case framing changes).

    The gRPC-Gateway envelope ``{"result": ...}`` is unwrapped before yield.
    """
    buf: list[str] = []
    async for raw in response.aiter_lines():
        if raw == "":
            if not buf:
                continue
            payload = "".join(buf)
            buf.clear()
            try:
                env = json.loads(payload)
            except json.JSONDecodeError:
                log.warning("bad JSON in stream payload: %s", payload[:200])
                continue
            yield env.get("result", env)
            continue
        if raw.startswith(":"):
            continue  # SSE comment / keepalive
        if raw.startswith("data:"):
            chunk = raw[5:]
            if chunk.startswith(" "):
                chunk = chunk[1:]
            buf.append(chunk)
            continue
        if raw.lstrip().startswith("{"):
            # Plain NDJSON line — emit immediately; some servers don't
            # bother with blank-line separators.
            try:
                env = json.loads(raw)
            except json.JSONDecodeError:
                buf.append(raw)  # might be a partial; keep buffering
                continue
            yield env.get("result", env)
            continue
        # Unknown framing line (event:, id:, retry:, …) — ignore.


# ---------- subscription worker --------------------------------------------


async def subscribe_kind(
    client: httpx.AsyncClient,
    kind: str,
    sink: LogstashSink,
    stop: asyncio.Event,
) -> None:
    """One worker = one identifier kind. Reconnects with backoff on failure."""
    backoff = 1.0
    while not stop.is_set():
        try:
            if kind in STATIC_IDENTIFIERS:
                # Bypass discovery — caller has supplied the identifier list.
                field = KIND_META[kind]["ids_field"]
                ids = [{field: i} for i in STATIC_IDENTIFIERS[kind]]
                log.info("[%s] using %d static identifiers (discovery skipped)", kind, len(ids))
            else:
                entities = await list_entities(client, kind)
                ids = to_identifiers(kind, entities)
            if not ids:
                log.info("no %s visible to api key — sleeping", kind)
                await _sleep_or_stop(REFRESH_INTERVAL, stop)
                continue

            log.info("[%s] subscribing to %d entities", kind, len(ids))
            body = {"identifiers": ids, "tail": 0, "names": [EVENT_NAMES_REGEX]}

            async with client.stream(
                "POST",
                f"{BASE}/events",
                headers={**HEADERS, "Accept": "text/event-stream"},
                json=body,
                timeout=httpx.Timeout(connect=10, read=None, write=10, pool=10),
            ) as r:
                if r.status_code != 200:
                    text = (await r.aread()).decode(errors="replace")[:500]
                    log.error("[%s] subscribe failed %d: %s", kind, r.status_code, text)
                    raise httpx.HTTPStatusError("bad status", request=r.request, response=r)

                # Re-subscribe periodically so new entities are picked up.
                refresh_at = asyncio.get_running_loop().time() + REFRESH_INTERVAL
                async for evt in iter_stream_events(r):
                    evt["_subscription_kind"] = kind
                    log.debug("[%s] event %s", kind, evt.get("name"))
                    await sink.write((json.dumps(evt, separators=(",", ":")) + "\n").encode())
                    if asyncio.get_running_loop().time() >= refresh_at:
                        log.info("[%s] refreshing subscription", kind)
                        break
                    if stop.is_set():
                        return
            backoff = 1.0  # successful run resets backoff
        except (httpx.HTTPError, OSError) as e:
            log.warning("[%s] stream error: %s — reconnect in %.1fs", kind, e, backoff)
            await _sleep_or_stop(backoff, stop)
            backoff = min(backoff * 2, 60)
        except Exception:
            log.exception("[%s] unexpected error", kind)
            await _sleep_or_stop(5, stop)


async def _sleep_or_stop(seconds: float, stop: asyncio.Event) -> None:
    try:
        await asyncio.wait_for(stop.wait(), timeout=seconds)
    except asyncio.TimeoutError:
        return


# ---------- main ------------------------------------------------------------


async def main() -> None:
    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)

    sink = LogstashSink(LS_HOST, LS_PORT)

    verify = not TTS_INSECURE
    if TTS_INSECURE:
        log.warning("TTS_INSECURE=true — TLS verification disabled (dev only!)")

    async with httpx.AsyncClient(verify=verify, http2=False) as client:
        workers = [
            asyncio.create_task(subscribe_kind(client, kind, sink, stop), name=f"sub-{kind}")
            for kind in KINDS
            if kind in KIND_META
        ]
        log.info("started %d subscription workers: %s", len(workers), ", ".join(KINDS))
        await stop.wait()
        log.info("shutting down...")
        for w in workers:
            w.cancel()
        await asyncio.gather(*workers, return_exceptions=True)
        await sink.aclose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)
