# API traffic protection

These ceilings mitigate sustained extraction. They are separate from the
three-guest/five-free-account public profile allowance and do not enforce
subscription entitlements or make public content uncopyable.

Defaults per network client, shared across API GETs and read-only explorer POSTs:

| Budget | Minute | Hour |
| --- | ---: | ---: |
| All applicable reads | 240 | 6,000 |
| Explorer read POSTs, additionally | 60 | 1,800 |

Over-budget responses are HTTP 429 with Retry-After and private/no-store.
CORS still wraps these responses. Normal successful responses are unchanged.
The limits can be tuned with MIMIR_API_REQUESTS_PER_MINUTE,
MIMIR_API_REQUESTS_PER_HOUR, MIMIR_API_BULK_REQUESTS_PER_MINUTE and
MIMIR_API_BULK_REQUESTS_PER_HOUR. MIMIR_API_TRAFFIC_LIMITS=off disables only
this limiter; it does not remove reload or export authorization.

Verified Google/Bing GETs are exempt only when both the claimed crawler and
the actual client IP match their official published ranges. The bundled
snapshot is refreshed in a background thread daily; network failures retain
the last known good ranges. Requests do not wait for DNS or feed fetches.

Mimir's shared public SSR reads are exempt using the existing server-only
MIMIR_EXPORT_PROXY_SECRET in X-Mimir-Export-Key, limited to GET /api/public/.
Deploy the frontend header change before activating these origin limits.
Do not put this secret in NEXT_PUBLIC variables or browser fetches. Origin,
Referer, arbitrary Authorization, forwarded-IP and claimed JA4 headers do not
grant an exemption. The ASGI peer is supplied by the existing Render/Uvicorn
proxy configuration; changing that proxy trust needs separate review.

Health/status, CORS preflight and operational POSTs are outside these read
budgets. Reload already requires MIMIR_RELOAD_SECRET or stays disabled.
Exports retain their separate service authorization and plan allowance.

Counters are bounded, in memory, and per process. The current production
service uses one Uvicorn worker. Restarts reset counters; multiple workers or
instances need a shared limiter before treating the budgets as global.
Rotating residential IPs can evade per-client budgets: keep the existing
Vercel scraper-fingerprint challenge. Account-wide limits require verified
account identity at the origin and remain separate work.

Validate locally with:

    python -m unittest test_api_traffic_protection test_reload_authorization -v

Production verification should inspect the deployed commit and test normal
public pages, signup, bounded API reads and the rejected anonymous reload.
Do not flood the live data API to test its threshold; threshold and bypass
behaviour are covered with an isolated ASGI test application.
