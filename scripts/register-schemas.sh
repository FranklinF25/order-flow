#!/usr/bin/env bash
# Register all event schemas in Schema Registry (RecordNameStrategy subjects:
# one subject per record full name) and set BACKWARD_TRANSITIVE compatibility.
set -euo pipefail
cd "$(dirname "$0")/.."

SR="${SCHEMA_REGISTRY_URL:-http://localhost:8081}"

for f in schemas/*/*.avsc; do
  subject=$(python3 -c "import json; s=json.load(open('$f')); print(s['namespace'] + '.' + s['name'])")
  payload=$(python3 -c "import json; s=json.load(open('$f')); print(json.dumps({'schemaType': 'AVRO', 'schema': json.dumps(s)}))")
  result=$(curl -sf -X POST "$SR/subjects/$subject/versions" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d "$payload") || { echo "FAILED: $subject ($f)"; exit 1; }
  curl -sf -X PUT "$SR/config/$subject" \
    -H 'Content-Type: application/vnd.schemaregistry.v1+json' \
    -d '{"compatibility": "BACKWARD_TRANSITIVE"}' >/dev/null
  echo "registered: $subject -> $result"
done
