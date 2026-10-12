## Context

XR-021 CONTRACTS.md R5-11 and R5-12 are the source of truth. This file records the local decisions.

## Decisions

- The resend wait is `least(cap, after * 2^least(resend_count, 60))` in `float8` seconds, converted by `make_interval(secs => ...)`. The exponent bound keeps the product finite (`2^60` times at most 86400 seconds), and the clamp happens before the interval exists, so the PostgreSQL interval range (about 178 million years) is never approached. The cap is at most 604800 seconds by configuration.
- An erasure request that is not published (`published_at is null and undeliverable_at is null`) is the only record of the sequences it names, so the tenant-scope removal set excludes it. Once published it is removed by a later deletion like any published row. The alternative, carrying the sequences into the new request, would rewrite a command row that the pump may be publishing at that moment.
- The residual gaps for a row in flight (a publish acknowledged before `mark_published` ran, a crash republish after the duplicate window, a row deleted between the pump's select and its publish) are recorded in `DEVELOPMENT.md` as accepted: closing them needs a row lock across the publish and the mark and a lock on the rows a deletion deletes, a larger change than this review round, and the seven day retention bounds the exposure.
