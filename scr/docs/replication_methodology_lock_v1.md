# Replication methodology lock v1

Date: 2026-07-18

This memo freezes the definitions used to decide whether the original import
analysis has been replicated and whether the same construction can be extended
through 2025. Historical v3 artifacts remain diagnostic; new work must use
versioned, source-separated artifacts.

## Separate scientific tracks

1. `package_import_pdf_gate`: the authors' package data, Stata-equivalent
   estimator, and frozen local PDF reference. This is the replication anchor.
2. `raw_outcome_point_estimate_gate`: raw Census outcomes compared with the
   package anchor while treatment and package policy are held fixed.
3. `raw_outcome_inference_gate`: confidence intervals and clustered inference
   compared on the same registered samples. This gate is distinct from point
   estimates because close curves can still have different uncertainty.
4. `raw_trade_archive_gate`: archive-native parsing and monthly reconciliation.
5. `raw_trade_outcome_extension_gate`: an independent outcome panel through the
   latest locally available month, with no package policy fields.
6. `independent_policy_gate`: legal product/date/rate reconstruction. Existing
   Section 301 mismatches remain evidence; this task does not alter semantics.
7. `section301_v5_ready` and `event_2025_ready`: downstream gates, both false
   until their predecessors pass.

The deprecated boolean `ready_for_extension` must not be used. Passing the
package numerical gate authorizes construction and validation of raw trade data;
it does not authorize policy or event-study claims.

## Outcome definitions

The canonical raw outcome panel preserves source fields and derives:

* value: `trade_value` in source units, with a separately documented scaling
  when compared to package `m_val`;
* quantity: primary quantity, preserving missing and zero indicators;
* pre-duty price: `trade_value / quantity` only when both are positive;
* duty-inclusive price: `(trade_value + cal_dut_mo) / quantity` only when the
  calculated-duty field is observed and both numerator inputs are valid.

`dut_val_mo` is retained as dutiable value and is not substituted for
`cal_dut_mo`. A statutory-rate multiplier is a diagnostic counterfactual, not
the canonical realized-duty outcome.

## Registered bridge gates

For each outcome and specification, use the same normalized keys, estimator
sample, event horizons, and cluster definition. Report Pearson correlation,
RMSE, maximum pointwise difference, interval-overlap (intersection length
divided by union length, excluding the normalized zero-width baseline from the
average but reporting it separately), and post-treatment sign agreement.

The registered thresholds are correlation >= 0.95, RMSE <= 1.25, maximum
difference <= 2.50, interval overlap >= 0.80, and sign agreement. Thresholds
must not be changed to obtain a pass. If point estimates pass but inference does
not, classify the result as an inference-gate failure rather than a replication
failure.

## Calendar rule

Keep three calendars explicit in every policy artifact: legal effective date,
paper-compatible nearest-month date, and any day-weighted realized-duty date.
The package bridge holds treatment timing and package policy fixed. Independent
policy comparisons are reported separately and cannot be used to repair the
outcome bridge.

## Extension rule

Native monthly HS10 is the canonical raw key. Any longitudinal concorded code is
a separately labeled derived field with a vintage, mapping direction, and loss
audit. Nominal values remain canonical; CPI-adjusted fields are separate and may
be null when CPI is unavailable. Every partition is ZSTD Parquet with source
archive/member/hash provenance and deterministic validation hashes.
