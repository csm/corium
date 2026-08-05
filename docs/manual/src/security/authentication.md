# Authentication and TLS

Every network surface establishes a request-scoped principal. Authentication
answers who is calling. [Authorization](authorization.md) answers what that
caller can do.

## The default is permissive

A server with no authentication flags does two things.

- It recognizes the shared development token, and it gives that caller the
  identity `operator`, vouched for by the provider `static-token`, with the
  role `admin`.
- It also admits an anonymous caller.

This default makes a local database usable with no flags. It is not safe on a
shared network.

> **CAUTION: Never expose a default-configured transactor or peer server
> outside a trusted network.** The shared development token is a compiled-in
> constant, and anonymous callers are admitted.

## Strict mode

Any of three flags switches a server to strict mode. In strict mode an absent
or unrecognized credential is rejected.

| Flag | Effect |
|---|---|
| `--serve-token <secret>` | Require this exact bearer token. It replaces the development token. |
| `--require-auth` | Require the development token, or `--serve-token`. Reject anonymous callers. |
| `--oidc-issuer <url>` | Accept tokens signed by this issuer, and the static token. |

`--serve-open` goes the other way. It disables authentication completely, and
every request arrives as anonymous. It conflicts with `--serve-token`,
`--require-auth`, `--oidc-issuer`, and `--authz-db`.

Read the secret from `CORIUM_SERVE_TOKEN` rather than from a process argument:

```sh
CORIUM_SERVE_TOKEN=$(cat /etc/corium/serve.token) \
  corium transactor --data-dir /srv/corium --require-auth
```

## Client tokens

A client sends its token with `--token`, or with `CORIUM_TOKEN`.

```sh
CORIUM_TOKEN=$(cat /etc/corium/serve.token) corium db list
```

`--token ""` connects anonymously. With no flag and no variable, the client
sends the shared development token.

## OIDC

`--oidc-issuer <url>` accepts bearer tokens signed by an OIDC issuer. The
static token keeps working alongside it.

| Flag | Effect |
|---|---|
| `--oidc-issuer <url>` | Issuer URL. |
| `--oidc-audience <aud>` | Accepted audience. Repeatable. Set it. |
| `--oidc-jwks-file <path>` | Read the JWKS from a file instead of fetching it. |

Two Cargo features apply. `oidc` verifies against a JWKS file. `oidc-discovery`
also fetches the JWKS from the issuer over HTTP. A binary without the feature
rejects the flags at startup and names the feature.

Set at least one audience. Without it, a token minted for another service of
the same issuer is accepted.

## TLS

A server terminates TLS when both certificate flags are present:

```sh
corium transactor --data-dir /srv/corium \
  --tls-cert /etc/corium/tls/server.pem \
  --tls-key /etc/corium/tls/server.key
```

A client enables TLS by naming a CA, a domain, or both:

```sh
corium db list --transactor https://txor-a:4334 \
  --ca /etc/corium/tls/ca.pem --tls-domain txor-a.internal
```

Three surfaces have no TLS of their own.

- The metrics endpoint. Keep it on a private operations network.
- The PostgreSQL wire server. Put a proxy in front of it.
- The storage backends use their own transport security. PostgreSQL uses
  `sslmode` in the URL. S3 uses HTTPS.

## Recommended settings

| Deployment | Settings |
|---|---|
| Laptop, single user | No flags. |
| Shared development host | `--require-auth`, or `--serve-token`. |
| Production, machine clients | `--serve-token`, TLS, `--authz-db`. |
| Production, human identities | `--oidc-issuer` with `--oidc-audience`, TLS, `--authz-db`. |

Authentication alone permits every action. Add
[authorization](authorization.md) to restrict what a principal can do.
