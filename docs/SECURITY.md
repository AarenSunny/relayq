# Security model

RelayQ defaults to an unauthenticated local-demo mode. Set `RELAYQ_API_KEY` to
require a bearer token for every state-changing request, including producer,
worker, and administrative operations.

```bash
RELAYQ_API_KEY='replace-with-a-random-secret' npm start
RELAYQ_API_KEY='replace-with-a-random-secret' npm run worker
```

Clients send the value in the `Authorization` header:

```text
Authorization: Bearer replace-with-a-random-secret
```

Token comparisons use constant-time byte comparison. The server also rejects
non-JSON request bodies, caps payloads at 1 MB, and sets defensive content-type,
framing, and referrer headers.

## Scope and limitations

The built-in key is intentionally a small deployment primitive, not a complete
identity system. Read endpoints remain accessible so the bundled dashboard and
health probes work without storing credentials in browser code. Do not expose
payloads or results containing sensitive data under this model.

An internet-facing deployment still needs TLS termination, secret rotation,
per-client identities, authorization scopes, rate limiting, and audit logs for
administrative changes. These limitations are explicit so the demo does not
claim production security it has not implemented.
