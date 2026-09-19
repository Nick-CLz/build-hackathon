# Insurance domain primer

A study aid for the data shapes in this repo. It covers the policy lifecycle,
the grain of each entity, how each one changes over time, and the analytics
questions the warehouse is built to answer. Thai motor and adjustable health
products get their own section because their quirks drive several design
choices downstream.

---

## 1. The lifecycle

```
quote ──(converts ~34%)──> policy ──> endorsement(s) ──> renewal / expiry
                              │                │
                              ├──> premium payments (instalments, refunds)
                              └──> claim(s) ──> reserve ──> settlement
```

Read it as two clocks running at once:

- **The contract clock.** A policy is sold once, then amended by endorsements,
  then either runs to expiry, is cancelled mid-term, or renews. The policy's
  *state* therefore changes over time, which is why policies get an SCD2
  snapshot rather than a simple overwrite.
- **The money clock.** Premium is *written* the moment the contract is bound,
  but it is *earned* gradually across the coverage period. Claims arrive
  unpredictably and are often reported well after the loss occurred. Almost
  every insurance metric is a ratio between these two clocks, which is why
  getting dates right matters more here than in most domains.

---

## 2. Entities

### customers
- **Grain:** one row per customer.
- **Key attributes:** `customer_id`, name, national ID, date of birth, contact
  details, address, province, country.
- **Change over time:** slowly. Address and phone change; identity does not.
  Treated as SCD1 here (latest wins) because no downstream metric depends on a
  customer's historical address.
- **Answers:** portfolio mix by age band and geography; the denominator for
  cross-sell and retention.
- **PII:** national ID (high), phone, email, DOB, address. All masked in gold.

### vehicles
- **Grain:** one row per insured vehicle.
- **Key attributes:** `vehicle_id`, plate and province, make, model,
  manufacture year, engine capacity, chassis number, declared value.
- **Change over time:** a policy's vehicle can be substituted mid-term via a
  `VEHICLE_CHANGE` endorsement, so the vehicle-to-policy link is not immutable.
- **Answers:** claim frequency by vehicle age and engine class; rating factor
  analysis; theft exposure by province.
- **PII:** plate number is personally identifying in practice (medium).

### partner_insurers
- **Grain:** one row per partner carrier.
- **Key attributes:** `partner_code`, legal name, country, commission rate,
  onboarding date, and — specific to this skeleton — a `schema_variant` that
  determines how their daily file is shaped.
- **Change over time:** rarely. Commission rates are renegotiated periodically.
- **Answers:** loss ratio and volume by partner; commission cost; which partner
  feeds are late or malformed.

### products
- **Grain:** one row per sellable product.
- **Key attributes:** `product_code`, English and Thai names, line of business
  (motor / health), cover type, base premium, default sum insured.
- **Change over time:** versioned in reality (rates are refiled); treated as a
  static dimension here.
- **Answers:** the primary slice for every profitability metric.

### quotes
- **Grain:** one row per quote issued.
- **Key attributes:** `quote_id`, customer, product, partner, channel, quoted
  premium, currency, status, and the policy it converted into (if any).
- **Change over time:** terminal. A quote converts, expires, lapses or is
  rejected, and is then immutable.
- **Answers:** **quote-to-policy conversion** by channel, product and partner;
  price elasticity; funnel leakage.

### policies
- **Grain:** one row per policy **per version** in silver (SCD2); one row per
  policy in the current-state view.
- **Key attributes:** `policy_id`, customer, vehicle, product, partner,
  inception and expiry dates, sum insured, written premium, currency, status.
- **Change over time:** this is the entity that moves. Endorsements change sum
  insured, coverage or vehicle mid-term; cancellations end it early. Changes can
  be **backdated**, arriving after a later state was already recorded.
- **Answers:** **written premium**, in-force count, average premium, **renewal
  rate**, exposure by month.

### endorsements
- **Grain:** one row per endorsement.
- **Key attributes:** `endorsement_id`, policy, type, **effective date**,
  **requested date**, old and new sum insured, premium delta.
- **Change over time:** immutable once issued, but the *effective* date may
  precede the request date — the backdating case.
- **Answers:** mid-term exposure change; premium adjustment leakage; how much of
  the book has been amended.
- **Why two dates matter:** `effective_date` drives exposure and earned premium;
  `requested_date` drives when we knew. Confusing them silently corrupts earned
  premium. This is the single most common insurance data bug.

### premium_payments
- **Grain:** one row per payment instalment (including refunds).
- **Key attributes:** `payment_id`, policy, instalment number, due date, paid
  date, amount, currency, method, status, refund flag.
- **Change over time:** a pending instalment becomes paid; a cancellation emits
  a negative pro-rata refund row.
- **Answers:** collection rate, days-to-pay, outstanding premium, cash vs
  written premium reconciliation.

### claims
- **Grain:** one row per claim.
- **Key attributes:** `claim_id`, claim number, policy, **loss date**,
  **reported date**, cause, status, gross incurred, paid, reserve, currency.
- **Change over time:** a claim is reported, reserved, revised, then settled or
  rejected. Incurred amounts move as reserves are re-estimated.
- **Answers:** **claim frequency** (claims ÷ exposure), **severity** (average
  cost per claim), loss ratio, and the **reporting-delay distribution**.
- **The hard part:** the gap between loss date and reported date. A claim that
  occurred in January may not arrive until March. Any pipeline that only
  processes "today's" rows will understate January's losses forever. That is
  what the configurable lookback window on the incremental claims model exists
  to fix, and what `mart_claims_lag` exposes.

### fx_rates
- **Grain:** one row per (from-currency, to-currency, date).
- **Key attributes:** rate date, currency pair, rate.
- **Change over time:** daily.
- **Answers:** enables any cross-country aggregate. Reporting currency is THB.
- **Design note:** conversion uses the rate **as at the transaction date**, not
  today's rate, so restating history stays stable.

---

## 3. The metrics, defined

| Metric | Definition | Trap |
|---|---|---|
| **Written premium** | Premium bound in the period, at inception | Includes the full annual premium even if only one month has elapsed |
| **Earned premium** | Written premium × elapsed fraction of the coverage period | Must respect endorsements and mid-term cancellations, not just inception/expiry |
| **Loss ratio** | Incurred claims ÷ earned premium | Comparing incurred claims against *written* premium flatters early months badly |
| **Claim frequency** | Claim count ÷ exposure (earned vehicle-years) | Using policy count instead of exposure distorts any partial-year book |
| **Claim severity** | Incurred ÷ claim count | Open claims carry reserves, not final costs; mixing settled-only understates it |
| **Quote-to-policy conversion** | Converted quotes ÷ total quotes | Must be attributed to the quote's period, not the policy's |
| **Renewal rate** | Renewed policies ÷ policies eligible to renew | The denominator is expiries in the window, not all policies |

**Earned vs written, concretely.** A THB 12,000 annual policy incepting 1 March
writes THB 12,000 in March but has earned only THB 1,000 by 31 March. Judge
March's loss ratio against the 1,000, not the 12,000.

---

## 4. Thai motor and adjustable health

### Compulsory motor — พ.ร.บ. (Por Ror Bor)
Mandated by the Motor Vehicle Victims Protection Act. Every registered vehicle
must carry it. It covers **bodily injury only** — never damage to the vehicle —
with capped statutory benefits, and the premium is fixed by vehicle class rather
than by risk. In the data this means low, tightly clustered premiums
(`MOTOR_CMI`, ~THB 645) and claims that are medical in nature. It is high volume,
thin margin, and effectively a compliance product.

### Voluntary motor — ชั้น 1 / 2 / 3
Sold on top of พ.ร.บ., with cover decreasing by class:

| Class | Thai | Own damage | Theft & fire | Third-party | Typical buyer |
|---|---|---|---|---|---|
| **1** | ชั้น 1 | Yes | Yes | Yes | New / financed cars |
| **2** | ชั้น 2 | No | Yes | Yes | Mid-age cars |
| **3** | ชั้น 3 | No | No | Yes | Older cars |

There is also 2+ / 3+, which add own-damage cover limited to collisions with
another vehicle. Not modelled here, but worth knowing they exist.

Class 1 dominates premium; class 3 dominates count. Any loss-ratio comparison
that ignores class is meaningless, which is why `mart_loss_ratio` is sliced by
product.

### Adjustable health plans
Cover where the insured can move the sum insured, deductible or benefit schedule
between periods, so the premium is re-rated at each adjustment. Consequences for
the data:

- Sum insured is **not** stable across the term — another reason for SCD2.
- Underwriting captures **declared pre-existing conditions** and a **BMI band**,
  which are health data and therefore *sensitive* personal data under PDPA.
  These are fully redacted outside restricted models.
- Claims are per-episode (admission, outpatient visit, surgery), so frequency is
  far higher and severity far lower than motor.

### Buddhist Era dates
Thailand uses the Buddhist Era calendar: **BE = CE + 543**, so CE 2026 is BE
2569. Thai partner systems export BE years in otherwise ordinary date formats,
sometimes with Thai month abbreviations (`6 ส.ค. 2568`). A BE year parsed as CE
lands ~543 years in the future and quietly poisons every date comparison. The
`be_to_ce_date()` macro handles this, and it is deliberately applied by
*detection* (year > 2400) rather than by partner, because partners are
inconsistent even within one file.

---

## 5. Deliberate data defects

The generator injects each of these on purpose. Every one is individually
toggleable (`LAKEHOUSE_MESS=none`, or a comma-separated subset), so any single
behaviour can be isolated. This table is generated from
`data_generator/config.py::MESS_REGISTRY`, and a test asserts the two agree.

<!-- MESS_TABLE_START -->
| Defect | What it looks like | Where it's handled |
|---|---|---|
| `exact_duplicates` | A partner resends a byte-identical row in a later file. | bronze keeps both; silver dedups on _record_hash |
| `near_duplicates` | The same business record resent with a corrected field and a newer timestamp. | silver dedup keeps latest per key with documented tie-break |
| `mixed_date_formats` | Dates arrive as YYYY-MM-DD, DD/MM/YYYY and D MMM YYYY in the same column. | silver staging coalesces multiple to_date patterns |
| `buddhist_era_dates` | Thai partners emit Buddhist Era years (2569 = 2026). | be_to_ce_date() macro in silver |
| `mixed_language_text` | Thai and English values in the same column (province, status, name). | silver mapping tables normalise to English codes |
| `whitespace_casing` | Trailing/leading whitespace and inconsistent casing on keys and codes. | trim + upper in silver staging |
| `late_claims` | A claim reported weeks after the loss date, arriving after its batch closed. | incremental claims model with a configurable lookback window |
| `backdated_endorsements` | A mid-term endorsement whose effective date precedes rows already captured. | SCD2 snapshot with check strategy; as-of-date model |
| `cancellations_refunds` | Policy cancelled mid-term with a negative pro-rata refund payment. | premium >= 0 singular test carves out refunds |
| `schema_drift` | A partner adds a new column in a later daily file. | bronze mergeSchema + drift audit table |
| `validation_failures` | Negative premium, claim outside coverage, orphan foreign keys. | quarantine at bronze; warn/error dbt tests at silver |
| `malformed_records` | Truncated CSV lines with no primary key, and non-JSON lines in a JSONL stream. | bronze quarantines them with a reason; they never reach silver |
| `mixed_currencies` | THB and IDR amounts in one column, needing fx conversion. | convert_currency() macro against fx_rates |
<!-- MESS_TABLE_END -->

Run `python -m data_generator --list-defects` to print the same registry.
