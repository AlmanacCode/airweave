# Cohere embedding adapter

2026-10-01. Implemented; SDK/HTTP transport tests only. No live inference,
quality benchmark, deployment change or existing-index migration claimed.

The existing dense embedder port now accepts an explicit batch purpose:
`document` (default for indexing) or `query`. Both current and legacy query
execution paths pass `query`; symmetric existing providers retain their behavior.
Cohere maps purpose to search_document/search_query. It uses the installed SDK,
requests float vectors, rejects truncation and validates count, dimensions and
finite values. Requests are sequential batches of at most96 texts with a60-second
transport timeout and SDK retries disabled; caller orchestration owns retries.
Provider error text is not copied into public exceptions.

Registry choices: cohere_embed_v4, cohere_embed_v5_pro, cohere_embed_v5_fast.
Configure COHERE_API_KEY, DENSE_EMBEDDER and EMBEDDING_DIMENSIONS explicitly.
The adapter validates the discrete dimensions supported by each model. These are
text registrations, not image-processing or cross-model Pro/Fast query support.
Use the existing deployment metadata mismatch guard; do not overwrite a MiniLM
index with Cohere vectors. Create an isolated model-qualified evaluation index
before changing runtime configuration. Retain the old runtime for comparison.

Official references:
- https://cohere.com/blog/embed-5
- https://docs.cohere.com/v2/docs/semantic-search-embed

The standalone import API remains to be designed and implemented: current source
provisioning requires a verified managed-provider identity. Wiki/session imports
must instead bind a trusted publisher and scope without pretending to be Composio
accounts. Retained revisions, fenced writes, indexing work and read authorization
should reuse existing store machinery. A new public write endpoint must not expose
writer fences or let callers nominate arbitrary organizations/principals.
