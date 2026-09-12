# Absurd Habitat

Habitat is a simple dashboard and monitoring tool for [Absurd](https://github.com/earendil-works/absurd), providing a web UI to visualize and monitor the state of running and executed tasks, queues, and events in a Absurd durable execution system.

It connects straight away to postgres.

<div align="center">
  <img src="screenshot.png" width="550" alt="Screenshot of habitat dashboard">
</div>

## Building

To build the complete application with the UI bundle embedded:

```bash
make build
```

This will:
1. Install UI dependencies and build the frontend assets
2. Compile the Go binary with embedded assets to `./bin/habitat`

## Running

Start the habitat server:

```bash
./bin/habitat run -db-name your-database-name
```

The dashboard will be available at `http://localhost:7890` by default.

## Configuration

Habitat can be configured via command-line flags or environment variables (prefixed with `HABITAT_`).

### Database Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `-db-url` | `HABITAT_DB_URL` | - | Full Postgres connection URL |
| `-db-host` | `HABITAT_DB_HOST` | `localhost` | Postgres host |
| `-db-port` | `HABITAT_DB_PORT` | `5432` | Postgres port |
| `-db-name` | `HABITAT_DB_NAME` | `absurd` | Database name |
| `-db-user` | `HABITAT_DB_USER` | - | Database user |
| `-db-password` | `HABITAT_DB_PASSWORD` | - | Database password |
| `-db-sslmode` | `HABITAT_DB_SSLMODE` | `disable` | SSL mode |

### Authentication Options

Habitat binds to loopback by default. A non-loopback listener is refused unless
both HTTP Basic credentials are configured. Keep the password in a secret
manager and terminate TLS at a trusted reverse proxy; Basic credentials must
not cross a plaintext network. The health endpoint remains unauthenticated for
platform probes and exposes only database availability.

| Environment Variable | Default | Description |
|----------------------|---------|-------------|
| `HABITAT_AUTH_USERNAME` | - | Operator identity required by the UI and API; must not contain `:` |
| `HABITAT_AUTH_PASSWORD` | - | Secret operator password (minimum 16 bytes); configure together with the username |

### Server Options

| Flag | Environment Variable | Default | Description |
|------|---------------------|---------|-------------|
| `-listen` | `HABITAT_LISTEN` | `127.0.0.1:7890` | Address to listen on; non-loopback requires authentication |
| `-base-path` | `HABITAT_BASE_PATH` | - | Serve UI/API under a URL prefix (e.g. `/habitat`) |

When Habitat is behind a reverse proxy, it also honors `X-Forwarded-Prefix` (plus
`X-Forwarded-Path` / `X-Script-Name`) to generate correct UI and API URLs.

## Verification

After building the frontend with `cd ui && npm ci --ignore-scripts && npm run build`,
run `go test -race ./...` and `go vet ./...` from `habitat`. The Go tests use an
in-memory scripted SQL driver; they do not connect to PostgreSQL. Authentication
integration tests parse the real configuration and construct the production mux,
covering UI/static/API access, credentials, health probes, and base-path routing.

The `Habitat authentication` workflow builds the real frontend, runs these checks
and the Go build, then demonstrates that the new contracts fail on the pinned
pre-authentication source. It also runs on contributor branches matching
`codex/habitat-auth-*`, allowing a fork to produce evidence without repository
secrets or privileged pull-request approval. These checks qualify source behavior;
they do not establish that a deployment has provisioned credentials or TLS.
