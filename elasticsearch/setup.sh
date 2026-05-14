#!/usr/bin/env bash
# Idempotent setup: kibana_system password + ILM policy + component template
# + index template. Runs as a docker-compose service on every `up`.
set -euo pipefail

ES="http://elasticsearch:9200"
AUTH="elastic:${ELASTIC_PASSWORD}"

# Wait for ES to be ready (compose healthcheck already gates us, but belt-and-braces).
until curl -fsS -u "$AUTH" "$ES/_cluster/health?wait_for_status=yellow&timeout=30s" >/dev/null; do
  echo "waiting for elasticsearch..."
  sleep 3
done
echo "elasticsearch is up"

# 1) Set kibana_system password so Kibana can connect.
echo "setting kibana_system password..."
curl -fsS -u "$AUTH" -X POST "$ES/_security/user/kibana_system/_password" \
  -H 'Content-Type: application/json' \
  -d "{\"password\":\"${KIBANA_PASSWORD}\"}" >/dev/null

# 2) Install ILM policy. RETENTION_DAYS comes from .env.
echo "installing ILM policy tts-events-ilm (delete after ${RETENTION_DAYS}d)..."
curl -fsS -u "$AUTH" -X PUT "$ES/_ilm/policy/tts-events-ilm" \
  -H 'Content-Type: application/json' \
  -d @- <<EOF >/dev/null
{
  "policy": {
    "phases": {
      "hot":    { "actions": { "rollover": { "max_age": "1d", "max_primary_shard_size": "20gb" } } },
      "warm":   { "min_age": "2d",  "actions": { "forcemerge": { "max_num_segments": 1 } } },
      "delete": { "min_age": "${RETENTION_DAYS}d", "actions": { "delete": {} } }
    }
  }
}
EOF

# 3) Component template — pins types for the common fields, keeps `data`
#    open so per-event-name payload variation does not cause mapping
#    explosions on indexed fields.
echo "installing component template tts-events-mappings..."
curl -fsS -u "$AUTH" -X PUT "$ES/_component_template/tts-events-mappings" \
  -H 'Content-Type: application/json' \
  -d @- <<'EOF' >/dev/null
{
  "template": {
    "settings": {
      "index.lifecycle.name": "tts-events-ilm",
      "index.codec": "best_compression",
      "index.mapping.total_fields.limit": 5000
    },
    "mappings": {
      "dynamic": "true",
      "properties": {
        "@timestamp":        { "type": "date" },
        "time":              { "type": "date" },
        "name":              { "type": "keyword" },
        "event_component":   { "type": "keyword" },
        "event_category":    { "type": "keyword" },
        "event_action":      { "type": "keyword" },
        "unique_id":         { "type": "keyword" },
        "origin":            { "type": "keyword" },
        "remote_ip":         { "type": "ip" },
        "application_id":    { "type": "keyword" },
        "gateway_id":        { "type": "keyword" },
        "gateway_eui":       { "type": "keyword" },
        "device_id":         { "type": "keyword" },
        "dev_eui":           { "type": "keyword" },
        "join_eui":          { "type": "keyword" },
        "dev_addr":          { "type": "keyword" },
        "organization_id":   { "type": "keyword" },
        "user_id":           { "type": "keyword" },
        "client_id":         { "type": "keyword" },
        "correlation_ids":   { "type": "keyword" },
        "_subscription_kind":{ "type": "keyword" },
        "context":           { "type": "object", "enabled": false },
        "visibility":        { "type": "object", "enabled": false }
      }
    }
  }
}
EOF

# 4) Index template — bind the component to the data-stream pattern.
echo "installing index template tts-events..."
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

# 5) Traefik access logs — ILM + component template + index template.
#    Separate from TTS events because the document shape is different
#    (per-request access record vs. TTS event envelope). Shares the same
#    retention window from RETENTION_DAYS.
echo "installing ILM policy traefik-access-ilm (delete after ${RETENTION_DAYS}d)..."
curl -fsS -u "$AUTH" -X PUT "$ES/_ilm/policy/traefik-access-ilm" \
  -H 'Content-Type: application/json' \
  -d @- <<EOF >/dev/null
{
  "policy": {
    "phases": {
      "hot":    { "actions": { "rollover": { "max_age": "1d", "max_primary_shard_size": "20gb" } } },
      "warm":   { "min_age": "2d",  "actions": { "forcemerge": { "max_num_segments": 1 } } },
      "delete": { "min_age": "${RETENTION_DAYS}d", "actions": { "delete": {} } }
    }
  }
}
EOF

echo "installing component template traefik-access-mappings..."
curl -fsS -u "$AUTH" -X PUT "$ES/_component_template/traefik-access-mappings" \
  -H 'Content-Type: application/json' \
  -d @- <<'EOF' >/dev/null
{
  "template": {
    "settings": {
      "index.lifecycle.name": "traefik-access-ilm",
      "index.codec": "best_compression",
      "index.mapping.total_fields.limit": 2000
    },
    "mappings": {
      "dynamic": "true",
      "properties": {
        "@timestamp":       { "type": "date" },
        "client_ip":        { "type": "ip" },
        "http_method":      { "type": "keyword" },
        "request_path":     { "type": "keyword" },
        "request_protocol": { "type": "keyword" },
        "request_scheme":   { "type": "keyword" },
        "request_host":     { "type": "keyword" },
        "router_name":      { "type": "keyword" },
        "service_name":     { "type": "keyword" },
        "service_url":      { "type": "keyword" },
        "status_code":      { "type": "short" },
        "response_bytes":   { "type": "long" },
        "duration_ns":      { "type": "long" },
        "user_agent":       { "type": "keyword" },
        "log_type":         { "type": "keyword" },
        "traefik":          { "type": "object", "enabled": false }
      }
    }
  }
}
EOF

echo "installing index template traefik-access..."
curl -fsS -u "$AUTH" -X PUT "$ES/_index_template/traefik-access" \
  -H 'Content-Type: application/json' \
  -d @- <<'EOF' >/dev/null
{
  "index_patterns": ["logs-traefik.access-*"],
  "data_stream": {},
  "composed_of": ["traefik-access-mappings"],
  "priority": 500
}
EOF

echo "setup complete."
