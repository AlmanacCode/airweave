# Retained Slack projection qualification

Frozen fork `f6c89e4` projected already-retained originals through the actual
Temporal workflow/activity, local MiniLM/BM25, and a unique collection in the
existing local Vespa. An isolated schema clone used the existing reprojection
operation from pipeline2 to3. Source payloads/revisions/visibility, scans and
capture cursors were unchanged; the original schema and collection were untouched.

All1,376 messages and80 channels now have current publications in the new
collection, including995 previously unindexed messages. There were no Slack
projection failures, zero-chunk publications or policy exclusions. Projection took
122.6 seconds and produced22.37MiB of derived text plus prepared index payload.
Bounds were600 seconds,512MiB derived payload and2GiB minimum free storage.

The installed CLI from product `1d3b9fd21` used actual Almanac routes/services and
fork HTTP with explicit synthetic authentication/account binding. Three newly
indexed messages were selected before ranking; keyword and hybrid searches found
all three. Exact-original reads matched retained payload/revision, and text reads
contained each native body. This is a known-item workflow proof, not a relevance
benchmark or live enrollment proof.

Capture remains active with discovery pending and no completed full capture.
Search correctly reports zero awaiting index and91 partially extracted messages.
The101 unique native files remain metadata-only. A CLI read verified an indexed
message body alongside `unavailable_original` / `original_not_captured`, with no
retained original file bytes. No attachment completeness is claimed.

No provider or paid-model calls, shared Vespa schema deployment or product
activation occurred. Guards rejected external HTTP and every remote deletion;
no GC worker ran. Exact new feed IDs were persisted before feeding. The isolated
clone/index remain for review; cleanup is not claimed. Private runner and detailed
artifacts: `/tmp/slack-retained-index-20261002/`. Safe counts are in the adjacent
JSON evidence file. Original SQL/publication count inheritance was checked
explicitly; other providers copied with the schema were not projected or included
in the final Slack-scoped counts.
