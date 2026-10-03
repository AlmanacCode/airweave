# Owned search excerpts and Vespa snippets

The schema exposes an optional query-time `query_snippet` dynamic summary from
`textual_representation`. The vector adapter preserves it separately from the full
chunk; it never replaces embeddings, originals or reranker input.

**Current owned search does not display this dynamic field.** Since commit
`9c59467`, `_matched_content` accepts only `ContentProvenance.preview` whose
matched part agrees with current extraction coverage. Preparation verifies exact
chunk/source offsets and retains up to600 Unicode characters of original content,
excluding generated metadata. Missing provenance produces no excerpt. Keyword,
hybrid and semantic requests currently share this behavior.

This preserves original-content attribution but can omit decisive text late in a
matched chunk. Existing dynamic-summary support is a candidate for a later
query-aware excerpt implementation, not evidence that current excerpts are
query-aware. Qualification must prove each displayed fragment belongs to the
attested original-content interval, preserve full reranker text, handle Unicode
and markup as plain text, and exclude stale/unsupported parts. Do not simply
restore a chunk-prefix or unverified marked-fragment fallback.

Vespa's default summary includes every summary-indexed field and fields declared
in any document summary. Keeping the default selection preserves child-schema
fields and canonical identity metadata alongside the new alias:
https://docs.vespa.ai/en/querying/document-summaries.html

The source is an index/summary field, not an attribute. Vespa documents adding a
new-name non-attribute summary alias as requiring reindexing:
https://docs.vespa.ai/en/reference/schemas/schemas.html#modifying-schemas
Inspect prepare actions before activation and follow Vespa's reindex procedure.
This does not require provider recapture or changing embedding models. No schema
has been deployed by this code change, and existing indexes safely omit the field.

Qualification gate: validate/deploy only an isolated application first; verify
inherited email/file schemas return both full text and the dynamic alias, with
unchanged canonical metadata. Exercise a keyword near the end of a long chunk,
hybrid lexical and vector-only matches, semantic-only fallback, and multilingual
text. Confirm full reranker input and stale-publication exclusion remain intact.
Historical dynamic-summary qualification (2026-10-02 UTC, before current
provenance-only display): the disposable CI application at
[c0632c2, run 36956517573](https://github.com/AlmanacCode/airweave/actions/runs/36956517573)
passed all six real-engine tests in 74.14 seconds. The new file/email case proves
default-summary alias availability, unchanged full chunks/identity/child URL,
near-end lexical highlighting, and adjacent Hindi/Chinese text. The canonical
projection test also verifies published search, semantic fallback, stale revision
exclusion, source access withdrawal, and deletion. These are synthetic correctness
checks, not corpus relevance measurements or an existing-index rollout.

The preceding run at bd2739d passed the new summary case but failed an older
assertion requiring a particular body sentence for a filename-matching query.
Actual Vespa selected the matching filename and returned `<sep />` delimiters.
The follow-up normalizes those delimiters and checks lexical query evidence while
retaining the full-body assertion for semantic fallback. No retained local index
was deployed, reindexed, or otherwise changed. Existing-index prepare/reindex
compatibility remains a rollout gate.

Current-source review (2026-10-03): `domains/search/owned.py::_matched_content`,
`domains/entities/canonical/content_models.py::ContentProvenance.chunk_preview`
and `test_unattested_lexical_fragment_is_not_content` establish the current
boundary. The historical CI result above qualifies Vespa's alias behavior, not
current product snippet quality. The frozen corpus and its judged excerpts were
not changed by this documentation correction.
