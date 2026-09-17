# Ask Mimir lifecycle monitoring

The durable `research_results` row is the canonical server outcome. Supabase and
PostHog events are observations; a missing browser event does not prove generation
failed. `ask_server_completed` and `ask_server_failed` describe generation.
`ask_answer_delivered` requires a browser render acknowledgement.

The API starts a background scan every 60 seconds over retained results (seven
days by default). It exposes content-free counts and the last check time under
`GET /api/health` → `ask_jobs.lifecycle_monitor`.

- A completed answer without a rendered receipt after 15 minutes logs
  `ask_delivery_unconfirmed`. It remains recoverable and retains its existing
  substantive-completion billing state. A later receipt clears the observation.
- A pending job older than the configured queue allowance plus execution limit
  plus five minutes logs `ask_orphaned_request`. A live worker is flagged but is
  not failed/refunded while it may still finish.
- An interrupted job with no current worker expires after that window. Its
  failed result and any remaining reservation refund are committed atomically;
  it cannot later resume under the same request ID. Its owner may submit again.
- Quota rejection uses `ask_request_rejected`, `reason=quota`,
  `technical_failure=false`. It is excluded from `server_failed` counts.
- Invalid saved records are flagged separately. Monitor errors set status=error;
  check the last successful `checked_at` as well as process availability.

Logs contain request IDs and classifications, never question/answer content.
Observations are emitted on transitions and may repeat after a process restart.
This is not an exactly-once event transport. Canonical result transitions and
receipt updates are idempotent. No external notification subscription is implied.

For analytics over older releases, exclude failure_stage=quota from technical
failure metrics. Treat accepted/started events older than the recovery window
without delivered/failed events as candidates requiring canonical-result review.
Historical releases without durable receipt support cannot establish delivery.
