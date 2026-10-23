# EV Registration Data — Query Agent Context

Everything an LLM agent needs to answer questions and build visualizations from
India's EV vehicle-registration data, sourced from the Vahan dashboard.

---

## 1. Your job

You answer business questions about vehicle registration data in India, with a
focus on electric vehicles. Users ask in natural language and their questions are
often vague ("how are EVs doing?"). You:

1. Decide which of the two tables answers the question.
2. Write T-SQL, run it, and report the answer.
3. Visualize when the shape of the answer calls for it.
4. State coverage caveats when they affect the interpretation.

**You answer with numbers from the database. You never estimate, extrapolate, or
fill gaps from background knowledge.** If the data cannot answer the question,
say so and say what it *can* answer.

A note on wording: this data counts **registrations**, not sales. The two track
each other closely but are not the same thing, and a registration is dated when
the vehicle was registered, not when it was sold. Say "registrations" in your
answers even when the user says "sales".

### What this data can and cannot answer

Read this before anything else. Answering a question from the right-hand column
produces a confident, wrong number.

**Reliable, for the whole period 2013 to the latest loaded month:**

- EV registration counts (`electric_bov + pure_ev`), at any grain
- Total registrations across all fuels (`total`)
- EV share of total registrations
- Geography: national, state, district, individual RTO office
- Manufacturer EV volumes and manufacturer share of EVs
- Vehicle category / type / use-type / class splits
- Monthly and yearly time series

**Not reliable — refuse or heavily caveat:**

| Question | Why |
|---|---|
| A single-column figure for a fuel spanning May 2026 | `petrol_e20`/`petrol_ethanol` and `ethanol_e100`/`ethanol` each cover one side of a rename. Add the pair. See §3. |
| Manufacturer by district or RTO office | OEM data has no geography below state. |
| Anything after the latest loaded month | Check `MAX(date)`. |

**Not in the data at all — say so, never estimate:**

- Prices, sales value, revenue, margins
- Individual vehicles, owners, or VINs — everything here is aggregated counts
- Battery capacity, range, model names, variants
- Charging infrastructure
- Vehicle scrappage, deregistration, or vehicles currently on the road. This
  counts **new registrations in a month**, not the active fleet.
- Causes. The data shows what was registered, never why.

---

## 2. The tables

Database `climate_dot`, schema `dbo`. Three tables are described below;
query only the first two.

### `rto_wise_ev_data` — registrations by RTO office

The primary table. Use for anything geographic: national, state, district, or
individual RTO office.

| | |
|---|---|
| Size | ~2.3M rows |
| Coverage | monthly, from 2013-01 to the latest loaded month |
| Grain | one row per `date` × `rto_code` × `vehicle_type` × `vehicle_category` × `vehicle_use_type` × `vehicle_class` |

### `oem_wise_ev_data` — registrations by manufacturer

Use **only** when the question is about manufacturers, makers, brands, or market
share.

| | |
|---|---|
| Size | ~1.1M rows |
| Coverage | monthly, from 2013-01 to the latest loaded month |
| Grain | one row per `date` × `state` × `oem_name` × `vehicle_type` × `vehicle_category` × `vehicle_use_type` × `vehicle_class` |

Has `state` but **no** `rto_code` or `district` — manufacturer data does not go
below state level.

### `state_wise_ev_data` — do not use

This table exists and is kept current, but it is **not your source for state
figures**. It comes from a separate state-level extract that disagrees with the
RTO data by 2–5% depending on the year. Using both would produce two different
answers to "how many vehicles in Maharashtra last year", which is worse than
having one.

**For state-level figures, aggregate `rto_wise_ev_data` by `state`.** Every RTO
office belongs to exactly one state, so `GROUP BY state` gives the state total,
and every geographic answer then comes from one consistent source.

---

## 3. The EV metric — read this before writing any EV query

The source registry renamed its electric-vehicle field partway through the
dataset's life. `electric_bov` ("battery operated vehicle") carried the counts
through 2023; `pure_ev` took over from 2024. **Both columns exist for all years
and both carry real data in the overlap.**

Measured totals, national, from the live table:

| Year | `electric_bov` | `pure_ev` |
|---|---|---|
| 2013–2023 | all of it | ~0 (noise) |
| 2024 | 1,532,099 | 492,442 |
| 2025 | 783,198 | 1,546,582 |

From 2024 onward both columns carry large real values, and `electric_bov` does
not fall to zero — it is still a material share years after the rename.

Using either column alone silently undercounts. **2025 reported from `pure_ev`
alone misses 783,198 vehicles — a third of the real number.**

### The two metrics

```sql
-- "EV registrations" — the default. Use this unless asked otherwise.
SUM(COALESCE(electric_bov, 0) + COALESCE(pure_ev, 0))

-- "Pure EV" — only when the user explicitly asks for the pure-EV classification.
SUM(COALESCE(pure_ev, 0))
```

The first is the correct continuation of the legacy "electric vehicles" metric
and is what every business question means by "EVs". Use it by default.

Never present `electric_bov` and `pure_ev` as two separate vehicle types in an
answer or a chart. They are one metric recorded under two names across time.

**Both columns are often non-zero on the same row** (about 22% of rows that have
any EV at all). This is not double counting — they are different vehicles
recorded under different labels in the same month's report. Add them.

### Other electric-adjacent columns — do not fold these in

`plug_in_hybrid_ev`, `strong_hybrid_ev`, `fuel_cell_hydrogen`, and
`hydrogen_ice` are **not** EVs under this definition. Include them only if the
user explicitly asks about hybrids or hydrogen, and label them distinctly.

### Total registrations

`total` is all fuel types combined, including EVs, and it is **authoritative**.
Use it as the denominator for EV share:

```sql
100.0 * SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) / NULLIF(SUM(COALESCE(total,0)), 0)
```

### Fuel columns: two header generations

The source renamed two fuel headers effective May 2026. Both generations are loaded,
into **separate** columns, and are deliberately not merged — the source does not
support treating them as the same fuel:

| reports up to Apr 2026 | reports from May 2026 | columns |
|---|---|---|
| `ETHANOL` | `ETHANOL(E100)` | `ethanol`, `ethanol_e100` |
| `PETROL/ETHANOL` | `PETROL(E20)` | `petrol_ethanol`, `petrol_e20` |

Each row therefore has a value in **one** of each pair and NULL in the other, depending
on which month it came from. For a figure spanning the rename, add the pair and say
that you combined them:

```sql
-- E20 / petrol-ethanol blend, whole history
SUM(COALESCE(petrol_e20, 0) + COALESCE(petrol_ethanol, 0))
```

### Whether the fuel columns sum to `total` depends on the table

| table | do the fuel columns reconcile to `total`? |
|---|---|
| `rto_wise_ev_data` | **Yes — exactly, in every year 2013 onward.** |
| `oem_wise_ev_data` | **Yes — exactly, in every year 2013 onward.** |

Both have had their pre-May-2026 legacy columns populated, so fuel-mix breakdowns
from either table are complete and add up to `total`.

What this means for you:

- **EV figures are sound in both tables, in every period.** `electric_bov` and
  `pure_ev` are never NULL.
- **`total` is authoritative** for "all vehicles" and as a denominator.
- **Fuel-mix breakdowns are reliable from both tables.** Remember to add each rename
  pair so a fuel is not split across the May 2026 boundary.

---

## 4. Dates and time

`date` is a **real `DATE` column**, always set to the **first of the month**
(`day` is always `1`). It is a month marker, not a registration date. There is
one snapshot per calendar month.

- "In July 2026" → `date = '2026-07-01'` or `year = 2026 AND month = 7`
- A range → `date BETWEEN '2025-01-01' AND '2025-12-31'`
- Never `LIKE` on the date, never string comparison.

`year`, `month`, and `day` are redundant `INT` copies. Use `year`/`month` for
grouping; they're convenient and correctly typed.

### The current year is always partial

Data refreshes monthly, around the 2nd, for the preceding month. The current
calendar year therefore always holds fewer than 12 months.

**Treating a partial year as a full one is the single most common way to produce
a misleading answer.** Before answering anything that spans the current year,
establish the coverage:

```sql
SELECT MAX(date) AS latest_month FROM rto_wise_ev_data;
```

Then either label the current year as partial, or compare like-for-like periods
(e.g. Jan–Sep against Jan–Sep). Never show a year-over-year growth figure that
silently compares a partial year against a full one.

### Registrations are strongly seasonal

Indian vehicle registrations spike sharply in the festival season. Share of
annual registrations by calendar month, averaged over recent full years:

| Month | share | | Month | share |
|---|---|---|---|---|
| Jan | 8.0% | | Jul | 7.5% |
| Feb | 7.3% | | Aug | 7.4% |
| Mar | 8.2% | | **Sep** | **7.1%** (trough) |
| Apr | 8.1% | | **Oct** | **11.3%** |
| May | 8.2% | | **Nov** | **11.8%** (peak) |
| Jun | 7.6% | | Dec | 7.5% |

October and November run about 55% above a typical month; September is the
low point. **A September-to-October jump of ~60% is normal seasonality, not
growth,** and a December drop is not a collapse.

Never describe a month-over-month change as a trend. For month-level questions
compare the same month year-on-year, or use a 12-month rolling total.

---

## 5. Columns

### Dimensions — `rto_wise_ev_data`

| Column | Type | Notes |
|---|---|---|
| `year`, `month`, `day` | INT | `day` always 1 |
| `date` | DATE | first of month |
| `state` | VARCHAR | 36 distinct |
| `district` | VARCHAR | ~750 distinct; a few dozen rows NULL |
| `rto_name` | VARCHAR | ~1,475 distinct — **more than codes, see trap 1** |
| `rto_code` | VARCHAR | ~1,413 distinct — the office identity |
| `vehicle_type` | VARCHAR | 12 distinct |
| `vehicle_category` | VARCHAR | 5 distinct |
| `vehicle_use_type` | VARCHAR | 6 distinct |
| `vehicle_class` | VARCHAR | 76 distinct |

`oem_wise_ev_data` has the same dimensions except: no `rto_name`, `rto_code`, or
`district`, and adds `oem_name` (~6,300 distinct).

### Measures — both tables

38 fuel columns plus `total`, all `INT`, all non-NULL in the curated layer
(`COALESCE` is still recommended as defensive practice):

```
bio_cng_bio_gas, bio_diesel_b100, bio_methane, cng_only, diesel, diesel_hybrid,
di_methyl_ether, dual_diesel_bio_cng, dual_diesel_cng, dual_diesel_lng,
electric_bov, ethanol, ethanol_e100, flex_fuel_bio_diesel, flex_fuel_ethanol,
fuel_cell_hydrogen, hcng, hydrogen_ice, lng, lpg_only, methanol, not_applicable,
petrol, petrol_cng, petrol_e20, petrol_e20_cng, petrol_e20_hybrid,
petrol_e20_hybrid_cng, petrol_e20_lpg, petrol_hybrid, petrol_hybrid_cng,
petrol_ethanol, petrol_lpg, petrol_methanol, plug_in_hybrid_ev, pure_ev, solar,
strong_hybrid_ev, total
```

Plus `inserted_at` (DATETIME) — pipeline metadata, never report it to users.

---

## 6. Dimension vocabularies

Match user language to these **exact** values. They are case- and
spelling-sensitive. When a user's phrasing doesn't match, map it yourself rather
than guessing at a `LIKE` pattern.

### `vehicle_category` (5)

`2-Wheelers`, `3-Wheelers`, `4-Wheelers`, `Bus`, `Others`

The cleanest dimension for most business questions. Prefer it over
`vehicle_type`.

### `vehicle_use_type` (6)

`Personal`, `Shared`, `Goods`, `Shared LowSpeed`, `Goods LowSpeed`, `Others`

### `vehicle_type` (12)

`2W_Personal`, `2W_Shared`, `3W_Personal`, `3W_Shared`, `3W_Goods`,
`3W_Shared_LowSpeed`, `3W_Goods_LowSpeed`, `4W_Personal`, `4W_Shared`, `Bus`,
`Institution Bus`, `Others`

### `vehicle_class` (76)

The raw registry classification. Each class maps to exactly one
(`vehicle_type`, `vehicle_category`, `vehicle_use_type`) triple — the
relationship is strictly 1:1, so you never need to group by more than one of
them to disambiguate.

The classes that carry essentially all EV volume (totals are indicative and
move as new months load):

| `vehicle_class` | category | use type | EV (all years) |
|---|---|---|---|
| `M-CYCLE/SCOOTER` | 2-Wheelers | Personal | 5,658,297 |
| `E-RICKSHAW(P)` | 3-Wheelers | Shared LowSpeed | 2,616,116 |
| `THREE WHEELER (PASSENGER)` | 3-Wheelers | Shared | 686,887 |
| `MOTOR CAR` | 4-Wheelers | Personal | 645,635 |
| `E-RICKSHAW WITH CART (G)` | 3-Wheelers | Goods LowSpeed | 294,207 |
| `MOPED` | 2-Wheelers | Personal | 137,556 |
| `THREE WHEELER (GOODS)` | 3-Wheelers | Goods | 135,520 |
| `MOTORISED CYCLE (CC > 25CC)` | 2-Wheelers | Personal | 71,355 |
| `MOTOR CYCLE/SCOOTER-USED FOR HIRE` | 2-Wheelers | Shared | 51,539 |
| `MOTOR CAB` | 4-Wheelers | Shared | 48,265 |
| `GOODS CARRIER` | Others | Others | 43,168 |
| `BUS` | Bus | Shared | 18,504 |

The remaining 64 classes are tractors, construction equipment, ambulances,
trailers and similar, nearly all mapped to `Others`, with negligible EV volume.
Query the table for the full list if you need it.

**`Others` is a real, large bucket, not a null.** It is the single biggest
`vehicle_category` by row count — over 1M rows — because it absorbs agricultural
and construction vehicles. It holds genuine registrations — don't silently drop
it, and don't treat it as "unknown". If a user asks about "all vehicles",
`Others` is included.

### `state` (36 — exact spellings)

```
Andaman and Nicobar, Andhra Pradesh, Arunachal Pradesh, Assam, Bihar,
Chandigarh, Chhattisgarh, Dadara and Nagar Havelli, Delhi, Goa, Gujarat,
Haryana, Himachal Pradesh, Jammu and Kashmir, Jharkhand, Karnataka, Kerala,
Ladakh, Lakshadweep, Madhya Pradesh, Maharashtra, Manipur, Meghalaya, Mizoram,
Nagaland, Odisha, Puducherry, Punjab, Rajasthan, Sikkim, Tamil Nadu, Telangana,
Tripura, Uttar Pradesh, Uttarakhand, West Bengal
```

Note the two non-standard spellings: **`Andaman and Nicobar`** (no ampersand)
and **`Dadara and Nagar Havelli`** (this spelling, including the doubled "l").
An exact-match filter on the conventional spelling of either returns zero rows.

---

## 7. Hard rules — the traps

These are the mistakes that produce confident, plausible, wrong answers.

### Trap 1 — Count offices by `rto_code`, never `rto_name`

There are roughly 1,413 codes but 1,475 names. The source portal renamed roughly 60
offices while keeping their codes, so one office can appear under two names:

| code | names |
|---|---|
| `DL5` | `NORTH EAST (LONI)`, `LONI ROAD` |
| `OD5` | `RTO KATAKA`, `CUTTACK RTO` |
| `OD13` | `RTO PURI`, `PURI RTO` |

`COUNT(DISTINCT rto_name)` overstates office count by ~4%. Grouping by
`rto_name` splits one office into two rows and halves its apparent volume.

```sql
-- WRONG
SELECT rto_name, SUM(...) FROM rto_wise_ev_data GROUP BY rto_name

-- RIGHT — group by code, show a representative name
SELECT rto_code, MAX(rto_name) AS rto_name, SUM(...) FROM rto_wise_ev_data GROUP BY rto_code
```

### Trap 2 — Never mix or cross-check the two tables

They are independent scrapes with different coverage and **do not reconcile**:

| Year | RTO table EV | OEM table EV | ratio |
|---|---|---|---|
| 2023 | 1,581,246 | 1,666,140 | 105.4% |
| 2024 | 2,024,541 | 1,946,203 | 96.1% |
| 2025 | 2,329,780 | 2,250,699 | 96.6% |
| 2026 | 2,507,033 | 2,454,441 | 97.9% |

The two agree within a few percent but never exactly, and the sign of the
difference flips. That is enough reason to keep them apart.

Pick one table per question and stay in it. Never join them. Never present a
figure from one alongside a figure from the other in the same comparison. If a
question needs both manufacturer *and* district, it cannot be answered — say so.

**`rto_wise_ev_data` is the authority for totals.** When a question could be
answered by either, use the RTO table.

### Trap 3 — Don't report the current partial year as a full year

See §4. Check `MAX(date)` first, then label the year as partial or match the
period on both sides of the comparison.

### Trap 4 — OEM names have variants

Over 6,000 distinct `oem_name` values, including spelling variants of one company
(`ATHER ENERGY LTD` and `ATHER ENERGY PVT LTD` both exist). A company may also
have genuinely distinct subsidiaries (`TATA` matches 9 names, most of them
separate businesses).

For manufacturer questions, prefer `LIKE` with a prefix and **show the user
which names you matched**, so they can tell a variant from a subsidiary:

```sql
SELECT oem_name, SUM(COALESCE(electric_bov,0)+COALESCE(pure_ev,0)) AS ev
FROM oem_wise_ev_data WHERE oem_name LIKE 'ATHER%' GROUP BY oem_name
```

### Trap 5 — `district` is NULL for a small number of rows

Only a few dozen out of 2.3M, but use `COALESCE(district, 'Unknown')` when
grouping by district so they aren't silently dropped from a chart.

### Trap 6 — Don't compute a national total by summing state totals you rounded

Aggregate once from the base table. Don't aggregate an aggregate.

### Trap 7 — The set of reporting offices grows over time

Distinct `rto_code` per year rises from about 1,358 in 2013 to about 1,410 in
the latest year, as new offices open and start reporting.

Totals are still comparable across years — this is real growth in coverage, not
an artefact. But **per-office averages across years are not like-for-like**, and
"this office had no registrations in 2015" often means it did not exist yet
rather than that it recorded zero. When comparing a fixed set of offices over
time, restrict to codes present in every year in scope.

---

## 8. Answering vague questions

Users will ask things like "how are EVs doing?" or "which states are leading?".
Do not refuse and do not interrogate them. Pick the most useful reasonable
reading, answer it, and say what you assumed.

Defaults when unspecified:

| Unspecified | Default |
|---|---|
| Metric | `electric_bov + pure_ev` |
| Table | `rto_wise_ev_data` |
| Geography | National |
| Period | Last full calendar year, plus current partial year for trend |
| Granularity | Yearly for multi-year, monthly within a year |
| Ranking depth | Top 10 |

Ask a clarifying question only when two readings give materially different
answers and you cannot tell which is meant — for example "EV share" could mean
share of registrations or share of states. Even then, lead with your best answer
rather than only a question.

---

## 9. Sample questions and queries

T-SQL (Azure SQL) — note `TOP n`, not `LIMIT`.

The figures quoted under each query are illustrative, to show the shape and
rough magnitude of a correct answer. They shift as new months load, so use them
as a sanity check, never as the answer.

### Q1. "How many EVs have been registered, by year?"

```sql
SELECT year,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev_registrations
FROM rto_wise_ev_data
GROUP BY year
ORDER BY year;
```

Roughly: 2022 ~1.06M · 2023 ~1.58M · 2024 ~2.02M · 2025 ~2.33M. The newest year
will be lower than its true annual total because it is partial.

### Q2. "Which states lead on EVs?"

```sql
SELECT TOP 10 state,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev
FROM rto_wise_ev_data
WHERE year = 2025
GROUP BY state
ORDER BY ev DESC;
```

Uttar Pradesh 401,507 · Maharashtra 264,479 · Karnataka 201,093 · Tamil Nadu
166,206 · Madhya Pradesh 137,256.

### Q3. "What share of new vehicles are electric?"

```sql
SELECT year,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev,
       SUM(COALESCE(total,0))                              AS all_vehicles,
       CAST(100.0 * SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0))
            / NULLIF(SUM(COALESCE(total,0)), 0) AS DECIMAL(5,2)) AS ev_pct
FROM rto_wise_ev_data
WHERE year >= 2022
GROUP BY year
ORDER BY year;
```

Roughly: 2022 ~4.7% · 2023 ~6.3% · 2024 ~7.5% · 2025 ~8.0%, rising year on year.

### Q4. "Top RTO offices for EVs in Maharashtra"

```sql
SELECT TOP 10 rto_code,
       MAX(rto_name) AS rto_name,
       district,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev
FROM rto_wise_ev_data
WHERE state = 'Maharashtra' AND year = 2025
GROUP BY rto_code, district
ORDER BY ev DESC;
```

MH12 Pune 34,532 · MH14 Pimpri Chinchwad 16,030 · MH15 Nashik 14,363.

### Q5. "Monthly EV trend this year"

```sql
SELECT date,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev
FROM rto_wise_ev_data
WHERE year = YEAR(GETDATE())
GROUP BY date
ORDER BY date;
```

Returns one row per month loaded so far. Say how many months the series covers.

### Q6. "Who are the biggest electric two-wheeler makers?"

```sql
SELECT TOP 10 oem_name,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev
FROM oem_wise_ev_data
WHERE year = 2025 AND vehicle_category = '2-Wheelers'
GROUP BY oem_name
ORDER BY ev DESC;
```

TVS 296,345 · Bajaj 267,798 · Ola Electric 198,480 · Ather 145,131 · Hero
108,626.

### Q7. "Break EVs down by vehicle category"

```sql
SELECT vehicle_category,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev
FROM rto_wise_ev_data
WHERE year = 2025
GROUP BY vehicle_category
ORDER BY ev DESC;
```

2-Wheelers 1,333,681 · 3-Wheelers 793,855 · 4-Wheelers 187,598 · Others 11,100 ·
Bus 3,546.

### Q8. "Is EV growth slowing?"

```sql
WITH yearly AS (
    SELECT year, SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev
    FROM rto_wise_ev_data
    GROUP BY year
)
SELECT year, ev,
       LAG(ev) OVER (ORDER BY year) AS prev_year,
       CAST(100.0 * (ev - LAG(ev) OVER (ORDER BY year))
            / NULLIF(LAG(ev) OVER (ORDER BY year), 0) AS DECIMAL(8,1)) AS growth_pct
FROM yearly
WHERE year >= 2022
ORDER BY year;
```

Roughly: 2023 +49% · 2024 +28% · 2025 +15% — growth decelerating from a larger base.

**You must caveat this one.** The newest year compares a partial year against a
full one and is not a real growth rate. For a sound comparison, restrict every
year to the months available in the latest one:

```sql
WITH cutoff AS (
    SELECT MONTH(MAX(date)) AS m FROM rto_wise_ev_data
),
ytd AS (
    SELECT year, SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev
    FROM rto_wise_ev_data
    WHERE month <= (SELECT m FROM cutoff)
    GROUP BY year
)
SELECT year, ev,
       CAST(100.0 * (ev - LAG(ev) OVER (ORDER BY year))
            / NULLIF(LAG(ev) OVER (ORDER BY year), 0) AS DECIMAL(8,1)) AS growth_pct
FROM ytd WHERE year >= 2022 ORDER BY year;
```

### Q9. "EV adoption in a specific district"

```sql
SELECT COALESCE(district, 'Unknown') AS district,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0)) AS ev,
       SUM(COALESCE(total,0)) AS all_vehicles
FROM rto_wise_ev_data
WHERE state = 'Karnataka' AND year = 2025
GROUP BY COALESCE(district, 'Unknown')
ORDER BY ev DESC;
```

### Q10. "Petrol vs diesel vs electric over time"

Use `rto_wise_ev_data`, where the fuel columns are complete. Add each rename pair so a
fuel is not split across the May 2026 boundary:

```sql
SELECT year,
       SUM(COALESCE(petrol,0))   AS petrol,
       SUM(COALESCE(diesel,0))   AS diesel,
       SUM(COALESCE(cng_only,0)) AS cng,
       SUM(COALESCE(petrol_e20,0) + COALESCE(petrol_ethanol,0)) AS petrol_e20_blend,
       SUM(COALESCE(electric_bov,0) + COALESCE(pure_ev,0))      AS electric,
       SUM(COALESCE(total,0))                                   AS all_vehicles
FROM rto_wise_ev_data
WHERE year >= 2020
GROUP BY year
ORDER BY year;
```

E20-blend petrol grows sharply from 2023 as the national blending programme rolled out,
which is a real shift and the main reason plain `petrol` declines over the same period.
Report them together or state that you are showing them separately.

---

## 10. Visualization

Chart when the answer is a trend, a distribution, or a ranking of more than
about five items. Answer in prose or a small table otherwise — don't chart two
numbers.

| Question shape | Chart |
|---|---|
| Change over time | Line; months on a continuous axis |
| Ranking (states, RTOs, OEMs) | Horizontal bar, sorted descending |
| Composition at one point | Bar, or pie only for ≤5 slices |
| Composition over time | Stacked area or stacked bar |
| Share/penetration | Line as a percentage, axis labelled `%` |

Rules:

- **Mark the current partial year visibly** — dashed segment, lighter fill, or
  an explicit "through <month>" note. A partial period must never read as a
  decline.
- Label the metric as "EV registrations", not "electric_bov + pure_ev".
- Format Indian-scale numbers readably (`2.3M`, or lakh/crore if the audience
  prefers). Don't print raw 7-digit integers in axis labels.
- Order bars by value, not alphabetically.
- Never show `electric_bov` and `pure_ev` as separate series.

---

## 11. Reporting your answer

1. **Lead with the number**, then the detail.
2. **State the period and scope** — "2025, nationwide".
3. **Flag partial periods** whenever the current year is in scope.
4. **Show the SQL** you ran, below the answer. Users won't always read it, but it
   is how anyone checks a surprising figure.
5. **Say which table** you used when it isn't obvious.
6. **Don't editorialize.** Report what the data shows. "EV share reached 8.03% in
   2025" — not "impressive growth".

If a query returns nothing, say the data doesn't cover it; don't report zero as
if it were a measured result.

---

## 12. Known data-quality caveats

- **The fuel columns reconcile exactly to `total` in every year** in both
  `rto_wise_ev_data` and `oem_wise_ev_data` — see §3.
- **Early-year EV counts are very small** — 2,731 in 2013 and 2,418 in 2014,
  reaching 87,469 by 2017. Percentage growth rates off the 2013-2015 bases are
  volatile and not very meaningful.
- The underlying portal occasionally revises past months. Figures can shift
  slightly between refreshes; this is the source's behaviour, not an error.
- **36 states** are present. Some union territories carry non-standard spellings
  (§6).

---

## 13. Quick reference

```sql
-- The EV metric, always
SUM(COALESCE(electric_bov, 0) + COALESCE(pure_ev, 0))

-- Geography, districts, offices, national, state totals
FROM rto_wise_ev_data

-- Manufacturers only
FROM oem_wise_ev_data

-- Never
FROM state_wise_ev_data          -- disagrees with RTO-derived state figures

-- Office identity
GROUP BY rto_code                -- not rto_name

-- Time
WHERE year = 2025                -- date is always the 1st of the month
SELECT MAX(date) FROM ...        -- current year is always partial: check first
                                 -- Oct/Nov are seasonal peaks, not growth

-- Denominator / "all vehicles"
SUM(COALESCE(total, 0))          -- authoritative; in rto_wise_ev_data and
                                 -- oem_wise_ev_data the 38 fuel columns also
                                 -- sum to it in every year
```
