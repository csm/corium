# Environment variables

Every variable below has an equivalent flag. A flag on the command line
overrides the variable.

## Corium variables

| Variable | Equivalent flag | Used by |
|---|---|---|
| `CORIUM_TOKEN` | `--token` | Every client command. |
| `CORIUM_SERVE_TOKEN` | `--serve-token` | `transactor`, `peer-server`, `postgres-server`. |
| `CORIUM_AUTHZ_DB` | `--authz-db` | `transactor`, `peer-server`, `postgres-server`. |
| `CORIUM_STORAGE_KEY` | `--storage-key` | `transactor`, `peer-server`, `postgres-server`, `gc`, `log`. |
| `CORIUM_STORE_PLUGINS` | `--store-plugin` | `transactor`, `store verify`. |
| `CORIUM_PLUGIN_READ_ONLY_CONFIG` | `--plugin-read-only-config` | `transactor`. |
| `CORIUM_POSTGRES_READ_ONLY_URL` | `--postgres-read-only-url` | `transactor`. |
| `CORIUM_S3_READ_ONLY_ACCESS_KEY_ID` | `--s3-read-only-access-key-id` | `transactor`. |
| `CORIUM_S3_READ_ONLY_SECRET_ACCESS_KEY` | `--s3-read-only-secret-access-key` | `transactor`. |
| `CORIUM_S3_READ_ONLY_SESSION_TOKEN` | `--s3-read-only-session-token` | `transactor`. |
| `CORIUM_S3_READ_ONLY_ROLE_ARN` | `--s3-read-only-role-arn` | `transactor`. |
| `CORIUM_S3_READ_ONLY_ROLE_EXTERNAL_ID` | `--s3-read-only-role-external-id` | `transactor`. |

`CORIUM_STORAGE_KEY` accepts a comma-separated list, because one process can
hold several keys. The same keyring resolves key-encryption keys and
[protection class keys](../security/protection.md).

`CORIUM_STORE_PLUGINS` accepts a path-separator-delimited list of files and
directories. Corium searches a directory for platform dynamic libraries only,
and it never adds the working directory.

## Rust and AWS variables

| Variable | Effect |
|---|---|
| `RUST_LOG` | Tracing filter, for example `corium_transactor=debug,corium_peer=info`. |
| `HOSTNAME` | Supplies the default `--owner` value, `transactor-$HOSTNAME`. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | Primary S3 credentials. |
| `AWS_PROFILE`, `AWS_REGION`, `AWS_ENDPOINT_URL` | Standard AWS configuration. |

The transactor takes its primary S3 credentials from the standard AWS chain,
which also covers instance and task roles.

## Handling secrets

Process arguments are visible to every user on the host. Three rules follow.

- Put a token, a password, or a connection URL in an environment variable, or
  in the [configuration file](../running/config-file.md).
- Set the mode of a key file and of a configuration file so that only the
  service user can read them.
- Prefer a `file:` storage key over an `env:` storage key. A file has a mode.
  An environment variable is inherited by child processes.
