# Introduction

Corium is a database system in the style of Datomic. It is immutable,
time-aware, and fact-oriented. Queries run in the application process against
an immutable database value. A single transactor process owns all writes to a
database.

This manual is for the person who runs Corium. It covers configuration,
initialization, keys, authorization, schema, backup, restore, availability,
and recovery. It does not teach Datalog query authoring. For the query
language, read the [query engine design
document](https://github.com/csm/corium/blob/main/docs/design/query-engine.md).

## How to read this manual

The manual has six parts.

- **Theory of operation** explains datoms, the log, the indexes, and the
  process roles. Read this part first. Every operational rule in the manual
  comes from one of these ideas.
- **Running Corium** covers installation, the transactor, storage backends,
  the database catalog, schema, and index publication.
- **Client surfaces** covers the console, the dashboard, the SQL shell, the
  PostgreSQL wire server, and the peer server.
- **Security** covers authentication, authorization, encryption at rest, and
  attribute protection.
- **Availability and data care** covers high availability, backup, restore,
  forks, and garbage collection.
- **Operations** collects the metrics and the runbooks.

The **Reference** part holds the command list, the environment variables, the
default values, and the glossary.

## Conventions

Commands appear as the `corium` binary. A source build runs the same command
as `cargo run -p corium-cli -- <command>`. The manual writes `corium` for
both.

Angle brackets mark a value that you supply, for example `<database>`.

This manual marks incomplete work in an aside. Two labels are used.

> **Not implemented.** The feature is specified in a design document, but no
> code implements it. Do not plan a deployment around it.

> **Partly implemented.** Some of the feature works. The aside states what
> works now and what does not.

Asides that carry neither label give background information.

## Terminology

This manual uses one word for one idea.

| Word | Meaning |
|---|---|
| transactor | The process that owns writes for a database. |
| peer | A library, or a process, that holds a database value and queries it locally. |
| storage service | The blob store and the root store together. |
| database value | An immutable snapshot of a database at one basis. |
| basis, `t` | The transaction number that a database value covers. |
| datom | One fact: entity, attribute, value, transaction, and assert or retract. |

The [glossary](reference/glossary.md) holds the full list.
