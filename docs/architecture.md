# Architecture

> This document grows with the build. Each phase adds its section and the
> design decisions behind it. The final diagram lands here in Phase 7.

## The medallion pattern in one paragraph

Data arrives messy and gets progressively more trustworthy as it moves
through three layers: **Bronze** stores every raw event exactly as received
(the permanent audit copy — never edited), **Silver** holds cleaned,
deduplicated, correctly-typed data, and **Gold** holds business-ready tables
(a star schema: one `fact_payments` table plus dimension tables for
merchants, customers, dates). Analysts and dashboards only ever touch Gold.

## Event schema (produced by the generator)

| Field | Type | Notes |
|---|---|---|
| event_id | uuid string | unique per event |
| event_time | ISO-8601 string (UTC) | when the payment happened |
| merchant_id | string, `M####` | 200 fixed merchants; ~1% deliberately null |
| merchant_name / country / category | strings | denormalised onto the event |
| customer_id | string, `C######` | 5,000 fixed customers |
| amount | float | log-normal: many small, few large; ~1% deliberately negative |
| currency | EUR / GBP / USD | ~1% deliberately invalid ("EU") |
| payment_method | card / bank_transfer / wallet / direct_debit | |
| status | authorised / settled / declined / refunded | weighted realistic mix |

## Phase log

### Phase 0 — scaffold (done)
- Kafka 3.7 in **KRaft mode** (no ZooKeeper), single node, dual listeners:
  `localhost:9092` for the laptop, `kafka:19092` for other containers.
- Kafka UI on `http://localhost:8080` for visibility while developing.
- Generator produces keyed-by-merchant JSON events to topic `payments.raw`.
