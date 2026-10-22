# DataSQRL Cloud web app clickstream

A DataSQRL pipeline that collects page views from the DataSQRL Cloud web app and answers which
projects a user returns to, which pages are used most, how people move from page to page, how
many people use the product each day and week, and the raw page-view trail of a single user or a
whole organization.

The API is exposed over GraphQL, REST and MCP.

## The event

The pipeline accepts one event type through the GraphQL mutation
`PageView(event: PageViewInput!)`. The web app's server sends it; a browser never calls this
pipeline directly.

| Field | Type | Nullable | Meaning |
|---|---|---|---|
| `event_id` | STRING | no | one id per page view, the deduplication key |
| `session_id` | STRING | no | browser session id |
| `user_id` | STRING | no | opaque user id, set by the caller's server, never an email |
| `is_global_admin` | BOOLEAN | no | true for DataSQRL staff accounts |
| `org_id` | STRING | yes | organization id; null on pages outside any org (e.g. `/profile`, `/docs`, `/release-notes`, `/utilization`, admin area) |
| `org_name` | STRING | yes | organization slug as it appears in the URL; null on pages outside any org |
| `route_pattern` | STRING | no | route template, the grouping key for "which page" |
| `path` | STRING | no | resolved path, without query string or fragment |
| `project_id` | STRING | yes | null outside a project route |
| `deployment_id` | STRING | yes | null outside a deployment route |
| `tab` | STRING | yes | null on pages without tabs |
| `referrer_path` | STRING | yes | previous path in the app |

`event_time` is the Kafka record metadata timestamp in UTC, assigned when the mutation is accepted.
Client clocks are not used. Up to 60 seconds of lateness is tolerated.

The payload carries no email, name, IP address, user agent or free text.

## How it works

```
GraphQL mutation ─► Kafka topic ─► Flink ─► Postgres ─► Vert.x API (GraphQL/REST/MCP)
   PageView          (managed)     dedup      served       JWT authenticated
                                              queries
```

**Ingestion** (`connectors/pageview-{prod,local}.sqrl`). `PageView` is a DataSQRL managed Kafka
table, exposed as the write mutation. Kafka absorbs the write peak.

**Schema** (`connectors/pageview.sqrl`). `_PageView_schema` holds the twelve payload columns shared
by every environment. `event_time` and the watermark live on each per-environment connector table.

**Processing** (`web_clickstream.sqrl`).

- `_PageViewDedup` keeps the earliest arrival per `event_id`, because delivery is at least once.
- `_PageViewClean` adds a UTC day bucket and an epoch seconds column, both computed in Flink. Every
  served query reads this table.
- `_DailyUser` is the distinct day, user and org presence set behind DAU and WAU, built as a one day
  tumbling window so the window time becomes the table's own event timestamp. `org_id` is kept as
  a grouping key (not folded away) so `ActiveUsers` can apply the same caller org scoping as the
  other queries; `DailyUserTest` collapses it back with `DISTINCT` to pin the same day/user pairs
  as before.

**Serving.** The query functions take parameters, so they run at query time in Postgres.
Sessionization, dwell time and transitions use window functions (`LAG`, `LEAD`, `ROW_NUMBER`,
`SUM OVER`) that Postgres supports and Flink streaming does not. Gap and dwell arithmetic is plain
integers over the epoch seconds column, 1800 seconds being the 30 minute session gap, because
`TIMESTAMPDIFF` does not translate to the Postgres dialect.

**Sessions.** Events sharing a `session_id` belong to one session while consecutive events are at
most 30 minutes apart. A larger gap starts a new session, so a page pair spanning that gap is not
counted as a transition.

**Retention.** `ttl(30 days)` on `_PageViewDedup`, `_PageViewClean` and `_DailyUser` bounds both
operator state and how far back a query can answer. Raw events are kept for that whole horizon, so
unique counts (`uniqueUsers`, `uniqueSessions`, `sessionCount`) are counted exactly over the events
in the window: a user active on several days counts once, not once per day.

## Queries

| Query | Parameters | Returns |
|---|---|---|
| `TopProjectsByUser` \* | `userId?`, `fromTime?`, `toTime?` | `projectId`, `orgName`, `viewCount`, `sessionCount`, `lastViewedAt` |
| `TopPages` \* | `fromTime?`, `toTime?`, `includeAdmins` | `routePattern`, `viewCount`, `uniqueUsers`, `uniqueSessions` |
| `PageTransitions` \* | `fromRoutePattern`, `fromTime?`, `toTime?` | `toRoutePattern`, `transitionCount` |
| `SessionJourney` \* | `sessionId` | per event: `sequence`, `userId`, `orgName`, `sessionId`, `routePattern`, `path`, `projectId`, `deploymentId`, `tab`, `referrerPath`, `eventTime`, `dwellSeconds` |
| `UserTrail` \* | `userId?`, `fromTime?`, `toTime?` | per event, oldest first: `sessionId`, `routePattern`, `path`, `orgName`, `projectId`, `deploymentId`, `tab`, `referrerPath`, `eventTime` |
| `OrgTrail` \* | `orgName`, `fromTime?`, `toTime?` | per event, oldest first: `userId`, `sessionId`, `routePattern`, `path`, `orgName`, `projectId`, `deploymentId`, `tab`, `referrerPath`, `eventTime` |
| `ActiveUsers` \* | `fromTime?`, `toTime?` | per day: `day`, `dau`, `wau` |

\* Row-level scoped to the caller's own token, on top of the listed arguments — see [Authentication](#authentication).

Time bounds are half open, `[fromTime, toTime)`. Every query that takes a window makes both bounds
optional (`fromTime?`, `toTime?`): each is applied only when supplied, so omitting `fromTime` drops
the lower bound and omitting `toTime` drops the upper bound. Calling one with no window returns the
whole retained history (30 days), a sensible default given the 30-day retention cap. `TopPages`
excludes staff traffic unless `includeAdmins` is true. `SessionJourney` returns null `dwellSeconds`
on the last event of a session.

`TopProjectsByUser` and `UserTrail` also make `userId` optional (`userId?`): omitting it returns the
caller's own rows, resolved from their `sub` claim (see Authentication below) rather than failing or
returning nothing. This is for an MCP assistant asked "what have I been looking at" that has no way
to learn the caller's own id to pass as `userId` — it can simply omit the argument. Passing `userId`
explicitly keeps working exactly as before, including to ask about a colleague in a shared
organization, still subject to the same row-level scoping.

The three trail queries (`SessionJourney`, `UserTrail`, `OrgTrail`) return `referrerPath`, the
previous in-app path as the browser recorded it. It is null on the first view of a visit. Unlike
inferring the previous page from the row above, `referrerPath` is recorded inside a single tab and
always points at the real previous page, so it stays correct when a user browses in two tabs at once
and the rows interleave by time.

## Engines

`flink`, `postgres`, `kafka`, `vertx`. Kafka carries the ingestion mutation, Postgres serves the
queries and enforces retention. Iceberg and DuckDB are not enabled: there is no data lake export and
no subscription.

## Authentication

The Vert.x engine validates a JWT (`authKind: ["JWT"]`, issuer and audience checked) on every call.
The web app's own backend attaches the token before it calls this pipeline, so the token never
reaches a browser; checking it costs nothing on the write path and keeps anyone who merely learns
the URL from writing junk into the pipeline.

**Row-level scoping on every query function.** All seven query functions — `TopProjectsByUser`,
`UserTrail`, `OrgTrail`, `TopPages`, `PageTransitions`, `SessionJourney` and `ActiveUsers` — are
exposed to the Cloud assistant as a tool, so the rows a caller sees follow from their own token, not
from arguments like `userId`, `orgName` or `sessionId` they pass. The Auth0 access token carries:

| Claim | Meaning |
|---|---|
| `sub` | the caller's own id, the same value stored in `user_id` |
| `https://datasqrl.com/data_group_ids` | the organization ids (`org_id`) the caller belongs to |
| `https://datasqrl.com/global_admin` | `true` for DataSQRL staff |

Each of the seven queries reads these as hidden function arguments bound with
`METADATA FROM 'auth.<claim-name>'` (quoting the whole namespaced claim name after `auth.` reads it
verbatim, slashes and colons included — the same mechanism as a plain claim like `auth.sub`, not a
separate one). `sub` is declared `NOT NULL`, since every token carries it; `global_admin` and
`data_group_ids` are declared nullable, because Auth0 only puts `global_admin` on admin tokens and
only puts `data_group_ids` on tokens for a user who belongs to an organization — a non-admin token or
a token for a user in no organization simply lacks the claim. Declaring either of them `NOT NULL`
made the server reject the whole call with 403 (`Required claim '...' is not present`) for exactly
the ordinary, non-admin users these queries exist for. A missing `global_admin` claim is read as not
an admin (`COALESCE(..., FALSE)`), and a missing `data_group_ids` claim is read as an empty list
(`ARRAY_CONTAINS` against a null array returns null, which the surrounding `OR` treats as false) —
both fall through to the same "or own `sub`" branch a present-but-false/empty claim would. A row is
visible when the caller's `global_admin` claim is true, when the row's `org_id` is in the caller's
`data_group_ids`, or when the row's `user_id` equals the caller's `sub` — so a person always sees
their own page views, including the ones made outside any organization where `org_id` is null. `userId`, `orgName` and `sessionId` remain ordinary filters that narrow a result
the caller is already allowed to see; they can no longer be used to reach anyone else's data. On
`TopProjectsByUser` and `UserTrail`, `userId` is optional: when omitted, the filter falls back to the
caller's own `sub` claim, so "who am I" needs no id at all; when given, it narrows within what the
scoping rule already allows exactly as before.

For `TopPages`, `PageTransitions` and `ActiveUsers` the scoping filter applies before the
aggregation, so the counts they return are computed only over the rows the caller may see: a member
of one organization gets that organization's numbers, and only a global admin sees the platform-wide
total. `SessionJourney` applies the same filter to the session's events before reconstructing the
journey, so a session id belonging to an organization the caller is not in (and that they did not
author) now returns no rows instead of another person's full trail.

The auth arguments are injected by the server, never schema-visible or caller-suppliable: the
GraphQL/REST/MCP signatures of these seven queries are unchanged.

In tests, a token's claims decide what a query is allowed to return (see `web_clickstream-api/tests/org_trail.graphql`,
`org_trail_admin.graphql` and `org_trail_denied.graphql` for a member, an admin and a denied
non-member against the same `contoso` organization; `active_users_org_scope.graphql` for an
aggregate — `ActiveUsers` — whose counts differ between a member of `acme` and the suite's default
admin token in `active_users.graphql`; `session_journey_denied.graphql` for `SessionJourney`
returning no rows for a session belonging to an organization the caller is not in; and
`user_trail_no_userid.graphql` for `UserTrail` called with `userId` omitted against a non-admin
token, returning exactly that caller's own trail (identical snapshot to `user_trail.graphql`, which
pins the same trail by explicit `userId`); `org_trail_no_admin_claim.graphql` for a token that omits
the `global_admin` claim entirely rather than carrying it as `false` — the shape Auth0 actually
issues for a non-admin user — proving the call succeeds and returns exactly the acme-scoped rows
`org_trail.graphql` returns for the same caller; and `user_trail_no_group_ids_claim.graphql` for a
token that omits `data_group_ids` entirely — a user in no organization — proving `UserTrail`
still returns that caller's own trail (identical snapshot to `user_trail.graphql`) by falling through
to the "own `sub`" branch instead of failing the call or erroring out of `ARRAY_CONTAINS` on a null
array). The suite's
default token (`web_clickstream-test-package.json`) is a global admin, so tests that do not override
it with a `.properties` file keep seeing every row, with the auth check a no-op.

**Deployed API (`-prod`).** The backend already sends Auth0 access tokens, which Auth0 signs with
RS256, so the production overlay validates RS256 (a single `pubSecKeys` entry, `algorithm: RS256`).
No secret is committed to this repository: the key, issuer and audience are read from environment
variables that an operator sets at deploy time.

| Variable | What it is | Where its value comes from |
|---|---|---|
| `WEB_CLICKSTREAM_JWT_PUBLIC_KEY` | The RS256 **public** key, in PEM, used to verify the token signature (the `buffer`). It is public, not a secret, but is kept out of the repo so it can be rotated independently. | Auth0 tenant → the signing certificate's public key. Downloadable from `https://<your-tenant>/pem` or the JWKS at `https://<your-tenant>/.well-known/jwks.json` (convert to PEM). |
| `WEB_CLICKSTREAM_JWT_ISSUER` | The expected token `iss` claim. | Auth0 tenant domain as an issuer URL, e.g. `https://<your-tenant>/`. |
| `WEB_CLICKSTREAM_JWT_AUDIENCE` | The expected token `aud` claim. | The Auth0 API **Identifier** (audience) configured for this pipeline's API. |

**Local and test (`-local`, `-test`).** Both overlays carry a dummy symmetric key (HS256,
`dummy-issuer`/`dummy-audience`) so the write path exercises authentication exactly as production
will, without needing an Auth0 tenant. The test runner sends a matching `Authorization` header.

## Layout

```
web_clickstream.sqrl                     # main script: dedup, serving functions, table tests
web_clickstream-shared-package.json      # base config: engines, UTC time zone, API protocols
web_clickstream-test-package.json        # filesystem fixture, deterministic results
web_clickstream-local-package.json       # Kafka mutation run locally
web_clickstream-prod-package.json        # deployable overlay
connectors/
  pageview.sqrl                          # _PageView_schema, payload columns only
  pageview-prod.sqrl, pageview-local.sqrl  # Kafka ingestion, event_time from metadata
  pageview-test.sqrl                     # filesystem stand-in, physical event_time
  test-data/page_view.jsonl              # fixture events
web_clickstream-api/
  tests/                                 # GraphQL query tests
  tests-write/                           # PageView mutation tests
snapshots/                               # test snapshots
run-tests.sh                             # test entry point
```

## Tests

```bash
./run-tests.sh                        # query and table tests, the primary gate
./run-tests.sh --env local            # PageView write path against Kafka
./run-tests.sh --compile --env prod   # compile the deployable overlay
./run-tests.sh --list-invocations     # show what would run
```

The first run of a new or changed test writes its snapshot and reports failure by design. Re-run to
verify against the stored snapshot.
