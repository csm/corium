# The configuration file

`corium transactor --config <path>` reads storage selection and read-only
discovery credentials from one EDN file. A flag on the command line overrides
the value in the file.

The file holds one EDN map. Keys are plain keywords without a namespace. An
unknown key is an error, and the message names the key.

## Example

```clojure
{:store :s3
 :data-dir "/srv/corium"
 :s3-bucket "corium-prod"
 :s3-prefix "corium/"
 :s3-region "us-east-1"
 :s3-read-only-role-arn "arn:aws:iam::123456789012:role/corium-reader"
 :s3-read-only-role-duration-seconds 900}
```

```sh
corium transactor --config /etc/corium/transactor.edn
```

## Keys

| Key | Type | Equivalent flag |
|---|---|---|
| `:store` | Keyword: `:mem`, `:fs`, `:postgres`, `:turso`, `:s3` | `--store` |
| `:data-dir` | String | `--data-dir` |
| `:turso-path` | String | `--turso-path` |
| `:postgres-url` | String | `--postgres-url` |
| `:postgres-read-only-url` | String | `--postgres-read-only-url` |
| `:plugin-read-only-config` | String holding a JSON object | `--plugin-read-only-config` |
| `:s3-bucket` | String | `--s3-bucket` |
| `:s3-prefix` | String | `--s3-prefix` |
| `:s3-region` | String | `--s3-region` |
| `:s3-endpoint-url` | String | `--s3-endpoint-url` |
| `:s3-read-only-access-key-id` | String | `--s3-read-only-access-key-id` |
| `:s3-read-only-secret-access-key` | String | `--s3-read-only-secret-access-key` |
| `:s3-read-only-session-token` | String | `--s3-read-only-session-token` |
| `:s3-read-only-role-arn` | String | `--s3-read-only-role-arn` |
| `:s3-read-only-role-session-name` | String | `--s3-read-only-role-session-name` |
| `:s3-read-only-role-duration-seconds` | Integer | `--s3-read-only-role-duration-seconds` |
| `:s3-read-only-role-external-id` | String | `--s3-read-only-role-external-id` |

## What the file does not hold

The file covers storage only. It does not hold the listen address, the owner
identity, or the lease values. It does not hold the index pacing, the garbage
collection schedule, the authentication flags, or the storage keys.

`:store` names a built-in backend only. A plugin backend needs
`--store <kind>:<json>` on the command line, and the file carries no plugin
paths. See [storage plugins](storage.md#storage-plugins).

Put those on the command line, or in the unit file of the service manager.

> **Not implemented.** There is no configuration file for the peer server, the
> PostgreSQL wire server, or the client commands. They take flags and
> environment variables only.

## Protecting the file

The file can hold static secrets. Two rules apply.

- Set the file mode so that only the transactor user can read it. For example,
  run `chmod 600 /etc/corium/transactor.edn`.
- Prefer the file, or the environment variables listed in
  [environment variables](../reference/environment.md), over process
  arguments. Process arguments are visible to every user on the host.
