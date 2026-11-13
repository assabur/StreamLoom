#!/bin/sh
set -eu
clickhouse-client --host 127.0.0.1 \
  --user "$CLICKHOUSE_USER" --password "$CLICKHOUSE_PASSWORD" \
  --database "$CLICKHOUSE_DB" --multiquery < /opt/streamloom/schema.sql
