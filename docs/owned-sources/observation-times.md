# Observation and storage timestamps

Canonical current rows expose three nullable journal-backed projections alongside
`observed_at` (latest observation):

- `first_observed_at`: collector observation on canonical revision one.
- `revision_observed_at`: collector observation that produced the current revision.
- `first_stored_at`: database transaction time of the first canonical journal write.

Unchanged source checks advance only `observed_at`. A changed revision, deletion or
reappearance advances revision observation while preserving the first timestamps.
Native `source_created_at` and `source_updated_at` retain provider meanings. None
of these timestamps claims complete provider event history or physical commit time.
A transaction can commit late; a storage-time upper bound alone is not a stable
snapshot paging guarantee.

Migration 0019 projects unique, matching revision-one/current journal snapshots.
Absent, malformed or ambiguous history stays null. Old journal `created_at` used
an application clock, so it is not backfilled as database time. New journal writes
explicitly use PostgreSQL transaction time, and the first stored projection and
new snapshot use the same returned value. Historical snapshots are never rewritten.
No legacy Entity creation-time fallback is allowed.

Reads/listing use current row columns, not a journal lookup per result. The bounded
migration reads at most 500 current rows and their relevant journal revisions per
batch. This changes no capture hashes, record revisions, index generations or
preparation version, and requires no reembedding. Migration has only been exercised
in disposable schemas; frozen fixtures must remain on their matched old runtime.
Product DTO/generated contract adoption must precede activating the new fork wire.

## Next chronological browsing contract (proposed, not implemented)

Use SQL keyset browsing when the root search query is omitted and an explicit
chronological basis is selected; do not sort a relevance top-200 subset.
`--time imported` means `first_stored_at`; `--time observed` means latest observation.
`--after` is inclusive, `--before` exclusive, resolved to aware absolute instants
before request signing. Source-created/updated retain their existing separate axes.
Unknown timestamps cannot match a constrained range.

The cursor binds source/account selection, basis, bounds, sort direction and last
(timestamp, record UUID). Each page enforces current tenant/source/ancestor access;
Almanac rechecks account bindings and native authored authority after I/O. This is
live traversal: mutable observation/update times and late transactions can move
rows across pages. It does not promise snapshot completeness. Browse individual
originals with existing thread/session context links, without fabricating complete
groups from unseen pages. Calendar scheduled occurrences and Wispr meeting start
remain explicit provider-specific time meanings, not a universal activity clock.
