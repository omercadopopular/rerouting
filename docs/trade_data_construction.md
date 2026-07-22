# Trade-Data Construction

## Unit of observation

Raw Census detail records are parsed month by month and aggregated to partner country \(c\), HS10 product \(i\), and calendar month \(t\). HS codes are stored as zero-padded strings. A numeric Stata representation such as `801001090.0` is normalized to `0801001090`; scientific notation and ambiguous codes are rejected rather than guessed. Duplicate detail records are summed before the uniqueness check.

Every processed month records its archive name, ZIP member, source SHA-256, parser version, raw and output row counts, duplicate counts, and value reconciliation. Large outputs are ZSTD-compressed Parquet. Missing quantities and observed zeros have separate indicators.

## Outcomes

Let \(CIF_{ict}\) be general CIF import value in dollars, \(Q_{ict}\) the first quantity, and \(D_{ict}\) calculated duties. The paper-scaled outcome fields are

\[
m\_val_{ict}=\frac{CIF_{ict}}{10^6}, \qquad
m\_q1_{ict}=\frac{Q_{ict}}{10^6}.
\]

For strictly positive quantities,

\[
m\_p_{ict}=\frac{CIF_{ict}}{Q_{ict}}, \qquad
m\_pduty_{ict}=\frac{CIF_{ict}+D_{ict}}{Q_{ict}}.
\]

The duty-inclusive outcome therefore uses observed calculated duties from the Census archive. It is not generated as price times an independently reconstructed tariff rate. This distinction was essential to eliminating the earlier duty-inclusive-price discrepancy.

The event regressions use \(100\log(y_{ict})\). Multiplying value or quantity by a constant before taking logs changes only the absorbed level and not the event coefficients. Nevertheless, the million-unit convention is retained to mirror the Stata programs exactly.

## Historical replication sample

The fixed historical comparison uses the raw-outcome observations that can be aligned to the authors’ estimation keys. Package tariff variables are stripped before the canonical raw-outcome artifact is written. The authors’ policy anchor and independently reconstructed policy are joined in separate regression panels, making the source of each line auditable.

## Long-horizon trade sample

The longer-horizon design reads independently constructed archive-native trade through October 2020. It preserves the same CIF and calculated-duty definitions. It is stored separately from the frozen historical benchmark and does not overwrite the original \([-6,6]\) sample.
