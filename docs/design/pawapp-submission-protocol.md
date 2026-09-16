# PawApp vNext: durable Engine submissions

This contract lets a trusted PawApp backend start an independent analysis
without a Main Chat turn or an existing Engine session. The Engine owns
execution and its replay log; the QwenPaw Host owns task identity, scope checks,
Direct/Delegated presentation, and any Main Agent continuation.

This branch implements **submission protocol 1**. It does not declare a released
Engine version compatible with all of PawApp vNext. Adapters must probe
`GET /api/v1/capabilities/submissions` and require `protocol_version: 1`,
`durable_submissions: true`, and `event_replay: true`. Older Engines lack this
endpoint; JSON mode reports unsupported and returns 501 for submit/query/replay.
Do not silently fall back to non-idempotent session/chat creation.

## Submit and reconcile

```http
POST /api/v1/submissions
Authorization: Bearer <backend service token>
X-User-Id: <stable caller namespace>
Content-Type: application/json

{
  "protocol_version": 1,
  "submission_id": "sub_host_generated_id",
  "text": "Analyze revenue for the selected datasource",
  "datasource_id": "ds_revenue",
  "agent_id": "default"
}
```

`protocol_version` and `agent_id` default to 1 and `default`. The ID accepts
1–128 ASCII letters, digits, underscores, or hyphens. `text` and `datasource_id`
must be nonblank; unknown fields and unsupported protocol versions are rejected.
Datasource existence, model readiness, and analysis execution can still fail
after acceptance. Acceptance is not a readiness guarantee.

Successful acceptance and identical retries both return **202**:

```json
{
  "protocol_version": 1,
  "submission_id": "sub_host_generated_id",
  "state": "accepted",
  "run": {
    "session_id": "ses_generated_id",
    "run_id": "chat_generated_id",
    "status": "running",
    "last_sequence_number": -1,
    "error": null,
    "reason": null
  }
}
```

The `submissions` SQL table has a unique `(user_id, submission_id)` key and
stores a request digest plus immutable session/run IDs. The digest covers the
effective protocol version, agent, text, and datasource. The receipt, new
session, and first chat commit in **one transaction**. Only the transaction
winner schedules the runtime; concurrent identical retries return its mapping.
Reusing an ID with different inputs returns **409**. Different submission IDs
create different sessions, allowing concurrent delegations to the same App.

HTTP request cancellation does not cancel the acceptance/scheduling operation.
A process exit after commit but before scheduling leaves an accepted run that
startup recovery marks interrupted; it never schedules a replacement run.
This is deduplicated acceptance, not an exactly-once guarantee for external
tool side effects.

`GET /api/v1/submissions/{submission_id}` returns:

| Outcome | Meaning for the Host adapter |
|---|---|
| 200, `state: accepted` | Bind the original session/run IDs; attach using the last cursor committed by Host. |
| 200, `state: not_found`, `run: null` | No receipt exists in this caller namespace. Retry with the **same** ID and inputs only if Host has never bound a run. |
| Timeout, 5xx, invalid response, unavailable Engine | Submission outcome is **unknown**. Keep the original ID and reconcile later; never reinterpret transport failure as `not_found`. |
| Accepted with `run.status: unknown`, `reason: run_missing` | Receipt remains, but its run is unavailable. Preserve the mapping and report unresolved; do not create a replacement. |

Receipts have no TTL and are not deleted when a session is soft-deleted. If a
run is missing, replay returns 410 and retries still return the original receipt.
Durability assumes the same persistent database: deleting/restoring/replacing
that database is not a supported transparent failover operation.

## Replay and recovery

`GET /api/v1/submissions/{submission_id}/events` resolves the receipt under the
same caller namespace and reuses the existing chat SSE stream. `run_id` is the
Engine's `chat_id`; it is not a second execution identity. Every persisted frame
contains session/chat IDs and a dense, monotonic `sequence_number` (starting at
0); its SSE `id` is the cursor. Pass `Last-Event-ID` or `after_sequence_number`
on reconnect; when both are present, the larger cursor wins.

Host must persist its task→session/run mapping and consumed cursor with its own
task/event update. `last_sequence_number` in lookup is an Engine watermark,
**not** permission to skip events Host has not consumed. Reconnect can redeliver
events; consumers deduplicate by run identity and sequence.

| Failure | Result |
|---|---|
| Host disconnect/reconnect, Engine still running | Replay from Host's committed cursor, then follow live events; no new analysis. |
| Engine restart with a persisted terminal response | Preserve its outcome and replay the same terminal event. |
| Engine restart with an unfinished accepted run | Atomically persist cancellation plus an error with `details.reason: executor_restarted`; submission status is `interrupted`. No automatic rerun. |
| Chat row says `completed`, but no terminal response exists | While running, lookup says `reconciling`; after restart, `interrupted`. The runtime may have lost final message data before flushing it. Never synthesize success. |
| Persisted failed/canceled chat lacks its final response | Startup appends the corresponding terminal event without executing again. |

For wire compatibility, interrupted runs use a `response` event with
`status: cancelled`, `error.code: VALIDATION`, and
`error.details.reason: executor_restarted`. The adapter maps that reason to
Host `interrupted`, separately from user cancellation. Lookup statuses are
`running`, `reconciling`, `succeeded`, `failed`, `cancelled`, `interrupted`, or
`unknown`. A successful outcome requires a persisted `response/completed`;
**EOF is never success**. Result text remains in the existing message/content
events and must be reconstructed by the adapter, including full snapshots
versus deltas.

Startup recovery runs before requests or scheduled jobs and commits chat state,
any missing terminal event, and its sequence watermark together. Recovery can
run repeatedly without appending duplicate terminal events. Existing console
and session/chat APIs retain their behavior; their POST endpoints do not gain
idempotency from this protocol.

## Trust and deployment boundary

Run one Engine process/worker per database. SQL unique keys protect concurrent
submissions, but the execution registry and live event fan-out remain in one
process; this is not a multi-worker scheduler. SQLite/WAL is the verified
backend. The SQL uses SQLAlchemy's portable expressions, but PostgreSQL recovery
has not been exercised in this change.

All routes use the existing service bearer/loopback middleware. `X-User-Id`
partitions receipts as a **trusted backend identity stamp**, not an authenticated
end-user principal. A service token holder can choose that header. The Host
must derive a stable namespace from its trusted context, enforce user/workspace/
App permissions, and keep the Engine token out of the browser. This change
does not add tenant isolation to older session/chat endpoints.

## Validation and integration boundary

The API tests use real SQLite transactions and HTTP routes with a controlled
runtime; no model/provider calls are needed. They cover concurrent duplicate
submissions, input conflicts, separate runs and caller namespaces, transaction
rollback, lost responses, cancellation of HTTP requests, hard process exit
after commit, restart recovery, live disconnect/replay, retained receipts after
history deletion, unavailable databases, bearer checks, and unsupported JSON
mode. Existing host-core regression tests cover console/chat compatibility.

The next integration is the QwenPaw Host Data adapter: map its existing
`submit/query/attach` interface to these routes and turn replayed Engine output
into Host TaskEvents. Task-scoped answer/cancel receipts, Main Agent wake-up,
action dispatch, task cards, configuration readiness, and public/private
skill/tool access are separate implementation gates. The protocol probe above
must not be used to claim those gates are complete.
