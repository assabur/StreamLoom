# AGENT RULES

- Use conventional commits; prefer `feat:`, `chore:`, `fix:` prefixes.
- Keep Go code formatted with `gofmt` and module-per-service under `services/`.
- For Java/Kotlin, use Gradle with Java 17; format with default IntelliJ style.
- Tests should live alongside code (e.g., `src/test/java`).
- Prefer `Makefile` targets over raw docker-compose commands in docs.
- Repository layout:
  - `services/binance-ingestor`
  - `services/trade-streams`
  - `services/clickhouse-sink`
  - `services/api`
  - `clickhouse/` for schema and sample queries
  - `deploy/` for configs (Prometheus, Grafana, compose overrides)
- When adding new code, include minimal inline comments for assumptions.
- PR message should summarize key components updated and tests run.
