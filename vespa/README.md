# Owned lexical snippets

The base schema adds a query-time `query_snippet` dynamic summary sourced from
`textual_representation`. It does not replace stored text, embeddings, canonical
originals or reranker input. Owned search consumes marked fragments only for
keyword/hybrid requests; missing, unmarked or semantic results retain the bounded
chunk-prefix fallback. Highlight delimiters are removed and Vespa fragment
separators become plain ellipses. The strings are untrusted display text, not HTML.

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
Qualification evidence (2026-10-02 UTC): the disposable CI application at
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
