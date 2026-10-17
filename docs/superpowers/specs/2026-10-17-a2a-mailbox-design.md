# `a2a-mailbox` — Design Specification

**Date:** 2026-10-17  
**Project:** `a2a-mailbox`  
**Language & Runtime:** Go (100% Go standard library, zero external dependencies; compatible with `a2a` CLI and `a2a` Command Plugin contract)

---

## 1. Intent & Why

Standard A2A servers (`a2a server --exec`) couple request reception with synchronous or streaming execution. In many human-in-the-loop, ticket-driven, or asynchronous event-driven architectures, an endpoint is needed whose **only job** is to receive incoming A2A requests, assign/track `taskId` and `contextId`, and persist or publish the task for asynchronous fulfillment.

`a2a-mailbox` is an executor-less A2A server and CLI plugin (`a2a mailbox`) that registers incoming A2A messages and stores them in one of three pluggable backends:
1. **`db` (Simple Local DB):** Atomic JSON-backed task store (`mailbox.json`).
2. **`tk` (`tk` Ticket Tool):** Creates and updates tickets in `.tickets/` via the `tk` CLI (`tk init`, `tk create`, `tk add-note`, `tk close`, `tk query`), syncing ticket statuses (`open`, `in_progress`, `closed`) directly with A2A task states (`submitted`/`input-required`, `working`, `completed`).
3. **`pubsub` (Cloud Pub/Sub):** Publishes incoming A2A requests as structured Cloud Pub/Sub messages (`POST /v1/projects/{project}/topics/{topic}:publish`) for production event-driven pipelines while maintaining state for `GetTask` and `ListTasks`.

---

## 2. Goals, Non-Goals, and Anti-Goals

### Goals
- **Coexist Seamlessly with `a2a-select` and `a2a`:** Implement the `a2a-mailbox info` command plugin contract so `a2a mailbox` and `a2a select` work side-by-side in `a2a`.
- **Full A2A v1.0 HTTP+JSON Protocol Compliance:**
  - `GET /.well-known/agent-card.json`
  - `POST /message:send`
  - `GET /tasks` (supporting `contextId` and `status` filters)
  - `GET /tasks/{id}`
  - `POST /tasks/{id}:cancel`
- **Three Pluggable Backends (`db`, `tk`, `pubsub`):**
  - `db`: Zero-setup local file database.
  - `tk`: First-class integration with `tk` (`--backend tk --tk-dir <dir>`), creating a ticket per A2A task with `--external-ref <taskId>` and `--tags a2a,context:<contextId>`, adding notes on follow-up turns, and syncing status bidirectionally when `tk start` or `tk close` is run.
  - `pubsub`: Publishes base64-encoded JSON task events with attributes (`taskId`, `contextId`, `backend`, `state`) to Google Cloud Pub/Sub REST endpoints (or local Pub/Sub emulators).
- **Operator CLI Commands:**
  - `a2a-mailbox serve` — Start the A2A mailbox server (bound to `127.0.0.1` by default).
  - `a2a-mailbox list` — List recorded tasks in the mailbox.
  - `a2a-mailbox show <task-id>` — Inspect a task's messages, context ID, and `tk` ticket ID.
  - `a2a-mailbox reply <task-id> "<text>" [--state completed|input-required|working|failed]` — Post a response artifact/status to a queued task and sync with `tk` / `pubsub`.

### Non-Goals
- Running an LLM or arbitrary shell command from incoming network payloads.

---

## 3. Security Architecture

- **Loopback Binding Default:** `a2a-mailbox serve` binds to `127.0.0.1` by default (`--host 127.0.0.1`).
- **Safe Command Execution (`tk` Backend):** Invokes the resolved `tk` binary via `exec.CommandContext` with discrete argument slices—never shell interpolation (`sh -c`).
- **Path Boundary Enforcement:** All database and `.tickets` directory paths are resolved with `filepath.Abs` and `filepath.Clean`, and files are written with `0o600` permissions (`0o700` for directories).
- **Bounded Request Bodies:** All HTTP handlers enforce `http.MaxBytesReader` / `io.LimitReader` (4 MiB max) to prevent memory exhaustion.
