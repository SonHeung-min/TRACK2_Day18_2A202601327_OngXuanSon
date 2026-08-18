# Architecture Brief: LLM Observability at 1B Requests per Day

## 1. Problem Statement

A foundation-model API team needs observability for 1B requests per day. Each raw request/response log is about 5 KB, so Bronze receives about 5 TB/day before compression. The platform must refresh tenant cost and latency dashboards every 5 minutes, keep full prompt/response payloads for 7 days for incident review, keep aggregates for 1 year, redact PII before any analyst can read data, and keep total storage below about $5K/month. The hard part is not only scale; it is balancing ACID correctness, fast tenant-level analytics, short hot retention for sensitive text, long aggregate retention, and clear provenance for debugging model and policy changes.

## 2. Architecture Diagram

```text
API Gateway / Model Serving
        |
        | append JSON events, request_id, tenant_id, model, policy_version
        v
Kafka / Kinesis topic: llm_request_events
        |
        | streaming ingest, schema validation, reject malformed records
        v
Bronze Delta table: bronze.llm_calls_raw
  partition: ingest_date
  retention: 7 days full prompt/response
  controls: PII tokenization, encryption, raw access denied by default
        |
        | dedup by request_id, parse tokens, normalize error codes
        v
Silver Delta table: silver.llm_calls_clean
  partition: event_date
  cluster/Z-order: tenant_id, model, event_ts
  columns: prompt_hash, response_hash, pii_token_ids, latency_ms, cost_usd
        |
        | 5-minute incremental aggregation
        v
Gold Iceberg/Delta tables
  gold.tenant_5m_metrics       -> dashboard refresh every 5 minutes
  gold.tenant_daily_metrics    -> 1-year retention
  gold.incident_trace_index    -> request_id, hashes, model_version, policy_version

Governance plane
  Catalog + table owners + access policies + lineage
  Audit table for every sensitive-data read

Maintenance plane
  Compaction, clustering, vacuum/expiry, orphan sweep, checkpoint monitor
```

## 3. Key Decisions and Rejected Alternatives

### Decision 1: Use Delta Lake for Bronze and Silver

I choose Delta Lake for Bronze and Silver because high-volume streaming ingest, schema enforcement, MERGE-based deduplication, time travel, and operational maintenance are central to this workload. Bronze must reject malformed schema changes, and Silver must support correction jobs without corrupting dashboards.

I reject plain Parquet because it has no transaction log, no reliable rollback, and no safe concurrent streaming writes. I reject a warehouse-only design because keeping 7 days of full prompt/response payloads in warehouse storage would be too expensive and would blur the security boundary between raw sensitive data and analyst-facing metrics.

### Decision 2: Use Iceberg or Delta for Gold, but expose Gold through the catalog

I choose catalog-managed Gold tables because dashboards, governance, and retention policy should be controlled at the table layer, not by path conventions. Iceberg is attractive for Gold because hidden partitioning and REST catalog interoperability work well for cross-engine reads. Delta is also acceptable if the organization already uses Delta deeply; the key is that Gold is registered, documented, and permissioned through the catalog.

I reject direct reads from `s3://bucket/path/date=...` because analysts will eventually depend on physical layout and bypass governance. I reject one giant observability table because dashboard queries only need aggregates, while incident review needs sensitive raw text for a short window.

### Decision 3: Partition by date, cluster by tenant and model

I choose date partitioning for Bronze/Silver and clustering or Z-ordering by `tenant_id`, `model`, and `event_ts` for Silver. Most queries filter by time window first, then tenant or model. Daily partitions keep retention and backfills simple; clustering keeps p95 dashboard queries fast.

I reject partitioning directly by `tenant_id` because high-cardinality tenants would create too many small files and uneven partitions. I reject only partitioning by date without clustering because a 5-minute tenant dashboard would scan too many files inside the hot date partition.

### Decision 4: Tokenize PII at Bronze ingestion

I choose PII detection and tokenization before Bronze becomes readable. Raw prompts and responses are stored encrypted, with detected PII replaced by stable tokens in the analyst-facing columns. A small restricted mapping table is owned by the security team and every read is audited.

I reject doing PII redaction only in Silver because raw Bronze would become an attractive and dangerous shortcut. I reject irreversible deletion of all sensitive strings at ingest because incident responders may need controlled access to reconstruct abuse or safety incidents during the 7-day window.

### Decision 5: Keep full payloads for 7 days, aggregates for 1 year

I choose a tiered lifecycle: Bronze raw payloads stay in hot object storage for 7 days, Silver normalized metadata stays for 90 days, and Gold aggregates stay for 1 year. After 7 days, prompt/response bodies are deleted or replaced with hashes and aggregate metrics.

I reject keeping full raw data for 1 year because 5 TB/day means about 1.8 PB/year before compression, which cannot fit the $5K/month target. I reject deleting everything after 7 days because finance, capacity planning, and model-quality teams still need long-term cost, latency, and error trends.

### Decision 6: Run maintenance as a product requirement, not an afterthought

I choose scheduled compaction, clustering, snapshot/log expiry, orphan sweeping, and checkpoint monitoring. NB6 showed that maintenance jobs are not cosmetic; they determine whether storage bills and query latency stay predictable.

I reject manual cleanup during incidents because by then dashboards are already slow and costs have already accumulated. I reject relying only on vacuum/expiry because uncommitted orphan files and stale metadata can survive unless explicitly swept.

## 4. Failure Modes

### Failure Mode 1: Schema drift breaks ingestion or silently corrupts dashboards

A model-serving team may add nested fields, change token-count types, or rename error codes. Detection comes from schema validation failures, Delta commit history, and dashboard anomaly checks. Rollback uses Delta time travel to restore the last valid Silver version, then a controlled schema evolution PR adds the new fields. Breaking changes are blocked until downstream Gold tests pass.

### Failure Mode 2: Raw PII becomes queryable by analysts

The worst security failure is a shortcut table or notebook reading Bronze prompts directly. Detection comes from catalog audit logs, data access policies, and daily scans for unapproved principals on Bronze. Rollback means revoking the policy, rotating exposed tokens if needed, and replaying affected Bronze data through the tokenizer into a corrected Silver version.

### Failure Mode 3: Small files make 5-minute dashboards miss SLA

Streaming micro-batches can create thousands of tiny files per tenant per day. Detection comes from file-count metrics, p95 query latency, and bytes scanned per dashboard query. Remediation is an automatic compaction job on hot partitions, followed by clustering by `tenant_id`, `model`, and `event_ts`. If a compaction job writes bad data, time travel restores the prior Silver version.

### Failure Mode 4: Stale external indexes return deleted incident data

If an incident search index points to payloads that lifecycle policy already deleted, responders may see inconsistent results. Detection comes from reconciliation between index IDs and live table versions. Rollback is to rebuild the index from a pinned table version, then enforce index entries that include table version, request hash, and expiry timestamp.

### Failure Mode 5: Retention jobs reduce metadata but not storage cost

Snapshot expiry can make the table history smaller while old data files or orphan manifests remain in object storage. Detection comes from comparing table metadata size, object-store bytes, and orphan-file scan results. Remediation is to pair snapshot/log expiry with an orphan sweep and a post-job cost report.

## 5. Back-of-the-Envelope Cost Estimate

Raw ingest is 1B requests/day times 5 KB, or about 5 TB/day before compression. Prompt/response text compresses well, so I assume 4:1 compression for Bronze object storage, giving about 1.25 TB/day compressed.

Full payload retention for 7 days:

```text
1.25 TB/day * 7 days = 8.75 TB hot Bronze
8.75 TB * $23/TB-month on S3 Standard ~= $201/month
```

Silver keeps normalized metadata and hashes for 90 days. Assume metadata is 20% of compressed Bronze:

```text
1.25 TB/day * 20% = 0.25 TB/day
0.25 TB/day * 90 days = 22.5 TB
22.5 TB * $12.5/TB-month on S3 Standard-IA ~= $281/month
```

Gold aggregates are tiny compared with raw logs. Assume 10K tenants, 20 models, and 288 five-minute windows per day. With compact Parquet/Delta/Iceberg storage, this is well under 1 TB/year:

```text
1 TB * $12.5/TB-month ~= $13/month
```

Maintenance and query compute are the real recurring cost. A daily compaction and clustering job over hot Silver partitions might process 1-3 TB/day. At roughly $0.04 per vCPU-hour and a 50-node short job budget, reserve about $2K/month for compute. Dashboard serving and ad-hoc incident review reserve another $1K/month.

Estimated monthly total:

```text
Bronze hot storage       ~= $201
Silver warm storage      ~= $281
Gold aggregate storage   ~= $13
Maintenance compute      ~= $2,000
Dashboard/ad-hoc compute ~= $1,000
Buffer for retries/audit ~= $1,000
Total                    ~= $4,495/month
```

This fits the $5K/month cap only if full prompt/response retention remains at 7 days and maintenance jobs prevent file-count growth. Keeping full payloads for 30 days would raise hot Bronze to about 37.5 TB and increase both storage and maintenance cost.

## 6. One-Week MVP Slice

The first shippable slice is not the entire platform. It is one tenant-level observability path that proves the architecture works end to end.

Day 1: define the event schema with `request_id`, `tenant_id`, `model`, `event_ts`, token counts, latency, status, cost inputs, prompt/response text, and `policy_version`.

Day 2: build Bronze ingestion into a Delta table with schema enforcement, PII tokenization, and a restricted raw table policy.

Day 3: build Silver deduplication by `request_id`, normalize model and error fields, and add prompt/response hashes.

Day 4: build Gold 5-minute and daily tenant metrics with p50/p95 latency, error rate, prompt/completion tokens, and cost.

Day 5: add compaction, clustering, vacuum/expiry, and orphan-sweep checks for the hot partitions.

Day 6: create a dashboard query that reads only Gold and proves refresh under 5 minutes on a 1-day synthetic load.

Day 7: run a failure drill: introduce a bad schema, restore the last valid Silver version, and show the dashboard recovers without reading raw Bronze.

The MVP is complete when one synthetic tenant can be ingested, redacted, aggregated, queried, rolled back, and lifecycle-managed with auditable evidence.
