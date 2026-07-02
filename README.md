# n8n-nodes-qod

An [n8n](https://n8n.io/) community node that runs SQL and browses the catalog
of a [Quack on Demand](https://github.com/starlake-ai/quack-on-demand) FlightSQL
edge — or any Apache Arrow Flight SQL backend (DuckDB, PostgreSQL, Dremio, etc.).

It talks to the edge over raw gRPC (`@grpc/grpc-js`, pure JavaScript, no native
addon) and decodes Arrow result streams with `apache-arrow`. The Flight +
FlightSQL protocols are inlined — no `.proto` files to ship.

---

## Resources and Operations

### Table — Zero-SQL CRUD

| Operation | Description | Input |
|-----------|-------------|-------|
| **Read** | SELECT rows (auto-generated SQL) | Tenant → Schema → Table → Columns → Filter → Limit |
| **Insert** | INSERT rows (single or batch) | Tenant → Schema → Table → Values (JSON) — or auto-map from upstream items |
| **Update** | UPDATE rows | Tenant → Schema → Table → Values (JSON) → WHERE clause |
| **Delete** | DELETE rows | Tenant → Schema → Table → WHERE clause |

- **Columns** dropdowns cascade automatically: `Tenant → Schema → Table → Columns`
- **Insert** receives JSON `{"col": value, …}` either from the `Values` field or
  auto-mapped from the input item keys. Multiple input items are batched into a
  single multi-row `INSERT` for performance.
- **Update / Delete** require a `WHERE` clause. `rows_affected` is always the
  real count (obtained via a `SELECT COUNT(*)` pre-flight).

### Query — Custom SQL

| Operation | Description |
|-----------|-------------|
| **Execute** | Run a `SELECT` / `RETURNING` statement |
| **Execute Update** | Run `INSERT`, `UPDATE`, `DELETE`, `CREATE TABLE`, or any DDL |

Empty result sets produce `{ "success": true, "message": "…" }`.

### Catalog — Browse without SQL

| Operation | Description | Fields shown |
|-----------|-------------|--------------|
| **List Tenants** | List available tenants (databases) | Tenant (shown but optional) |
| **List Schemas** | List schemas in a tenant | Tenant |
| **List Tables** | List tables/views in a schema | Tenant → Schema |
| **Describe Table** | List columns (name, type, nullable) | Tenant → Schema → Table |

All catalog operations use the native FlightSQL commands
(`CommandGetCatalogs`, `CommandGetDbSchemas`, `CommandGetTables`).

---

## Credentials

**Quack on Demand API:**

| Field | Default | Purpose |
|-------|---------|---------|
| Host | `127.0.0.1` | FlightSQL edge hostname or IP |
| Port | `31338` | FlightSQL edge port |
| Tenant | `acme` | Routing tenant |
| Pool | `bi` | Routing pool inside the tenant |
| User / Password | `admin` / `admin` | HTTP Basic credentials |
| Superuser | `true` | Authenticate against the system realm (bypasses per-statement ACL) |
| Use TLS | `true` | Edge listens with TLS (the default) |
| Verify TLS Certificate | `false` | When off, the auto-generated self-signed cert is accepted |

> **Connection test** (green checkmark on save) is not available because n8n
> only supports HTTP-based credential tests for community nodes loaded via
> `N8N_CUSTOM_EXTENSIONS`. To verify connectivity, use **Query → Execute**
> with `SELECT 1`.

---

## Install

### From npm (recommended)

Settings → Community Nodes → Install, then enter `n8n-nodes-qod`.
A self-hosted n8n instance is required (community nodes are not available on
n8n Cloud's verified-only mode unless verified).

### From source (development)

```bash
git clone https://github.com/starlake-ai/quack-on-demand.git
cp -r quack-on-demand/examples/n8n n8n-nodes-qod
cd n8n-nodes-qod
npm install --legacy-peer-deps --ignore-scripts
npm run build
```

Start n8n with the custom node loaded:

```bash
npm run dev
# → builds TypeScript, starts n8n at http://localhost:5678
```

Or watch-only (recompile on save, restart n8n manually):
```bash
npm run dev:watch
```

---

## Architecture

| Layer | Technology |
|-------|------------|
| Transport | gRPC (`@grpc/grpc-js`, pure JS) |
| Serialization | Protocol Buffers (`protobufjs`, inlined `.proto` source) |
| Data format | Apache Arrow IPC (`apache-arrow`) |
| TLS | `node:tls` — self-signed cert is pinned off the wire when verification is off |

### FlightSQL operations used

| Proto command | Used for |
|---------------|----------|
| `CommandStatementQuery` | SELECT, DML, DDL |
| `CommandGetCatalogs` | List tenants |
| `CommandGetDbSchemas` | List schemas |
| `CommandGetTables` (+ `include_schema`) | List tables, discover columns |

The node is compatible with any FlightSQL server, not just Quack on Demand.

---

## Notes and limitations

- **Self-hosted n8n required.** Community nodes cannot be installed on n8n Cloud
  unless the package is verified.
- **One statement per call.** The edge executes a single SQL statement; the node
  does not split multi-statement input.
- **No credential connection test.** n8n's `N8N_CUSTOM_EXTENSIONS` loader only
  supports HTTP-based `ICredentialTestRequest` — function-based gRPC tests are
  not dispatched. Use `SELECT 1` via the Query resource instead.
- **DuckLake tables do not support `RETURNING`.** The node uses a pre-flight
  `SELECT COUNT(*)` to report accurate `rows_affected` counts.
- **Bulk Insert not yet available.** QoD's FlightSQL edge currently only
  supports `CommandStatementQuery` and the catalog commands. `DoPut` (Arrow
  streaming) and `CommandStatementIngest` are not supported by the edge,
  so bulk ingestion via Arrow is not yet possible. The regular Insert operation
  already batches multiple input items into a single multi-row `INSERT`
  statement for good performance.
- **TLS.** When _Verify TLS Certificate_ is off, the edge's self-signed
  certificate is fetched from the wire and pinned. For production, install a
  CA-signed certificate and enable verification.

---

## License

Apache-2.0
