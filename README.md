# MiR_BRIDGE

An asyncio/FastAPI middleware service that bridges MiR (Mobile Industrial Robots) AMRs to a fleet-management system ("QAMS" / Mission Control) over RabbitMQ.

For each registered AMR, MiR_BRIDGE:
- Authenticates against the robot's MiR REST API and opens a ROS Bridge websocket to stream pose (`/tf`) and status (`/robot_status`).
- Establishes a session with Mission Control once the robot and RabbitMQ are both reachable.
- Exchanges heartbeats and control commands (map updates, emergency stop, mission status, path routing, etc.) with QAMS over RabbitMQ topic exchanges.
- Exposes a small REST API for managing the AMR registration table and syncing map data from robots.

## Architecture

```
Mission Control (QAMS) <--REST/RabbitMQ--> MiR_BRIDGE <--REST/WebSocket (ROS Bridge)--> MiR AMR
```

- `main.py` — entry point; loads the AMR registry from Mission Control, starts one `AMR` instance per robot, and runs the FastAPI/Uvicorn server.
- `src/service/amrs/` — per-robot logic: connection-state tracking (`amr.py`), heartbeat ping/pong (`heartbeat.py`), ROS Bridge + control-command handling (`status.py`).
- `src/service/rabbitmq/` — RabbitMQ connection management (`connect_impl.py`, `rabbit_client_io.py`), exchange/queue topology (`queues.py`), and outgoing-message builders (`transaction_wrapper.py`).
- `src/service/webService/` — FastAPI app, routes, and error handling.
- `src/types/` — all shared type definitions (Pydantic models / TypedDicts / enums), grouped by domain: `amr.py`, `ros.py`, `mission.py`, `messages.py`, `cmd_id.py`, `rabbitmq.py`, `web.py`.
- `src/configs/` — YAML-backed, Pydantic-validated configuration.
- `src/logger/` — Loguru setup with separate rotating sinks for general logs, MQ traffic, and heartbeats.

## Requirements

- Python >= 3.13
- [uv](https://docs.astral.sh/uv/) (recommended) or pip
- A running RabbitMQ broker
- A reachable Mission Control ("QAMS") host exposing the AMR registry API

## Installation

```bash
uv sync
```

This creates `.venv/` and installs dependencies from `uv.lock`.

## Configuration

Settings are read from `src/configs/config.yaml` and validated by `src/configs/config.py`.

| Key | Description | Default |
| --- | --- | --- |
| `RABBIT_MQ_HOST` | RabbitMQ broker host | `127.0.0.1` |
| `RABBIT_MQ_PORT` | RabbitMQ AMQP port | `5672` |
| `RABBIT_MQ_UI_PORT` | RabbitMQ management UI port | `15672` |
| `RABBIT_NODE_NAME` | RabbitMQ node name (informational) | — |
| `RABBIT_MQ_USER` / `RABBIT_MQ_PASSWORD` | RabbitMQ credentials | — |
| `MISSION_CONTROL_HOST` / `MISSION_CONTROL_PORT` | Mission Control (QAMS) API location | — |
| `MIR_ACCOUNT` / `MIR_PASSWORD` | Credentials used to call each robot's MiR REST API | `distributor` / `distributor` |

Copy/edit `src/configs/config.yaml` before running the service.

## Running

```bash
uv run main.py
```

On startup the service:
1. Polls Mission Control for the AMR registry, retrying every 3 seconds until it succeeds.
2. Starts a background task per registered AMR to authenticate and connect it.
3. Serves the REST API on `0.0.0.0:4008`.

Logs are written under `~/kenmec/_logs/kenmec_bridge/` (general + MQ logs) and `~/kenmec/_logs/kenmec_bridge/heartbeat/` (heartbeat-only), both with daily rotation.

## API

| Method | Path | Description |
| --- | --- | --- |
| `GET` | `/all_mir_amr` | List all registered AMRs (id + IP) |
| `POST` | `/create_mir_amr` | Register a new AMR |
| `PUT` | `/update_mir_amr` | Update an existing AMR entry |
| `DELETE` | `/delete_mir_amr` | Remove an AMR entry |
| `GET` | `/sync_map` | Fetch map metadata directly from each registered robot |

All successful responses are wrapped as `{ success, code, message, data }`; errors follow a consistent `{ success: false, error: { code, message, ... } }` shape.

## Development

Lint/format rules are configured in `pyproject.toml` (line length 100, single quotes) for [ruff](https://docs.astral.sh/ruff/), which isn't a project dependency — run it via `uvx`:

```bash
uvx ruff check .    # lint
uvx ruff format .   # format
```

## Packaging

The project can be packaged into a standalone binary with [PyInstaller](https://pyinstaller.org/) (a dev dependency, see `main.spec`):

```bash
uv run pyinstaller main.spec
```

This produces a single-file executable at `dist/main` (bundles `src/configs/config.yaml` alongside it). Run it directly instead of `uv run main.py`:

```bash
./dist/main
```
