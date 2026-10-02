# Owned lexical snippets

The base schema adds a query-time `query_snippet` dynamic summary sourced from
`textual_representation`. It does not replace stored text, embeddings, canonical
originals or reranker input. Owned search consumes marked fragments only for
keyword/hybrid requests; missing, unmarked or semantic results retain the bounded
chunk-prefix fallback. The strings are untrusted display text, not HTML.

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
Current tests simulate Vespa summary output; they do not prove live snippet
generation, highlighting defaults, or deployment compatibility.
