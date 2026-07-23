# February 2025 tariff pass-through extension

## Purpose and status

This extension applies the historical pass-through workflow to the tariff episode beginning in February 2025. It does not modify the locked 2018 replication. The unit of observation remains partner country, native ten-digit HTS product, and calendar month.

The project uses separate scientific gates:

1. **Trade gate.** Each Census archive must parse, reconcile, and produce a unique partner--HS10--month panel.
2. **Policy gate.** Every treatment component must have a reviewed official source, legal date, rate, partner scope, product scope, exceptions, and stacking rule.
3. **Identification gate.** The proposed treatment must not be absorbed by the declared fixed effects.
4. **Estimation gate.** Only validated trade and policy panels may enter the regressions.

As of the July 2026 build, official Census bulk archives exposed by the project cover through December 2025. The complete short window is therefore `[-6,+6]`; the support-limited persistence window is `[-6,+10]`. A full `+24` requires February 2027 trade data. The runner expands automatically as later official archives become available and never replaces unavailable horizons with zeros.

The current archive-native build covers the 17 months from August 2024 through December 2025. It contains 4,949,734 unique partner--HS10--month rows, has no duplicate canonical keys, and reconciles every monthly CIF total exactly to its source archive. A no-overwrite run reused all 17 partitions and reproduced source-set fingerprint `7c9e21...a7883` and partition-set fingerprint `f31c66...f14a`.

## Trade data

Monthly imports come from the U.S. Census Bureau [Merchandise Trade Imports](https://www.census.gov/foreign-trade/data/IMDB.html) fixed-width ZIP archives. For every archive, the builder records the archive and member names, SHA-256 fingerprint, source row count, period validation, partition row count, quantity states, and nominal CIF reconciliation.

Let \(CIF_{ict}\) be general CIF value, \(Q_{ict}\) first quantity, and \(D_{ict}\) calculated duty for product \(i\), partner \(c\), and month \(t\). Outcomes are

\[
m\_val_{ict}=CIF_{ict}/10^6,\qquad
m\_q1_{ict}=Q_{ict}/10^6,
\]

\[
m\_p_{ict}=CIF_{ict}/Q_{ict},\qquad
m\_pduty_{ict}=(CIF_{ict}+D_{ict})/Q_{ict}.
\]

Unit values are defined only for strictly positive quantities. Source missing quantities and source-coded zeros remain distinct. Across the current event window, the fixed-width field contains 2,144,118 zero values and no blank/missing values. A source-coded zero is treated as unavailable for a unit-value denominator; it is not interpreted as evidence that an economically meaningful quantity of zero was observed. Partitions are atomic ZSTD Parquet under `data/processed/trade/extension_2025/`; data artifacts are not committed.

## Policy data

The December 31, 2024 statutory schedule is the baseline. January 2025 Section 301 changes are retained as `pre_transition_2025_carry_in`: they affect the tariff level but are not attributed to the incoming administration or the February treatment.

The source ledger is built from official USITC, Federal Register, White House, USTR, Commerce, and CBP records. Principal actions include:

- China/Hong Kong IEEPA duty effective February 4, 2025 ([Executive Order 14195](https://www.federalregister.gov/documents/full_text/html/2025/02/07/2025-02408.html));
- the cumulative 20 percent China fentanyl component effective March 4 ([CBP implementation notice](https://www.federalregister.gov/d/2025-03677));
- reciprocal tariffs and Annex-II exclusions under [Executive Order 14257](https://www.federalregister.gov/documents/full_text/html/2025/04/07/2025-06063.html);
- the April China reciprocal escalation under [Executive Order 14266](https://www.federalregister.gov/documents/full_text/html/2025/04/15/2025-06462.html) and its May suspension under [Executive Order 14298](https://www.federalregister.gov/documents/full_text/html/2025/05/21/2025-09297.html);
- revised steel and aluminum Section 232 measures effective March 12 and June 4;
- automobile and automobile-parts Section 232 measures beginning April 3 and May 3;
- later 2025 suspensions, partner modifications, sectoral actions, exclusions, and reductions.

For a component \(j\) with rate \(r_j\), effective date \(s_j\), and terminal date \(e_j\), its monthly rate follows the locked historical arithmetic. The effective day is excluded in the initial month, so an action beginning on day \(d\) of a \(D\)-day month receives weight \((D-d)/D\):

\[
\bar r_{jt}=\frac{1}{D_t}\sum_{d\in t}r_j\mathbf 1\{s_j<d\leq e_j\}.
\]

The legal clock preserves \(s_j\). The paper-compatible event clock assigns the current month when the effective day is at most 15 and the next month otherwise. Tariff rates themselves always use exact legal days; the paper clock changes the event label, not the statutory path.

Legally exempt rows receive a zero increment for the relevant component. Unresolved quota/TRQ, specific, compound, content-based, or importer-specific rates remain null and are excluded from primary estimation. In particular, monthly HS10 data do not reveal metal content, non-U.S. automobile content, entry-level USMCA qualification, or quota tier.

The current nine-action source table is a reviewed inventory of principal actions, not a claim to be the complete 2025 ledger. The local archive contains 33 official 2025 HTS releases in addition to the annual tariff files and current Chapter 99 export. The immediate constraint is therefore unmaterialized and unreviewed product logic, not a demonstrated absence of all official source files. The unresolved items are the versioned reciprocal Annex-II product exclusions, derivative/content rules, automobile content and USMCA entry rules, all partner-rate modifications through December, and the independently reconstructed inherited MFN/201/232/301 baseline on the 2025 HS10 universe. The regression runner therefore remains blocked; no package-policy or realized-duty substitute is used to bypass this gate.

## Estimators and identification

Two applications are preregistered:

1. China and Hong Kong around the February shock.
2. All new-administration 2025 actions, using both a common-February clock and staggered first-increase clocks.

The exact historical fixed effects are retained as an identification diagnostic. A broad China-by-month treatment is absorbed by country--month fixed effects, so it is not an identified primary design. The adapted bilateral event specification is

\[
100\log y_{ict}=\sum_{k\ne-6}\beta_k Exposure_{ic}\mathbf 1\{event_{ic,t}=k\}
+\alpha_{ic}+\eta_{it}+\gamma_{c,y(t)}+\varepsilon_{ict},
\]

with partner--product, product--month, and partner--year fixed effects. Standard errors are clustered by partner and HS8.

The dynamic specification defines

\[
x_{ict}=\Delta\log(1+\tau_{ict})
\]

from the independently constructed total statutory tariff. It estimates six leads, the contemporaneous change, and either six or the support-limited number of lags, using the same exact-calendar lead/lag semantics and cumulative covariance calculation as the historical pipeline.

## Horizon rules

The short and long results are separate regressions:

- **Short:** observations are restricted to `[-6,+6]`; month `-6` is omitted. Later observations are dropped, not top-coded into `+6`.
- **Long:** observations are restricted to `[-6,H]`, where \(H=\min(24,\text{latest month}-2025m2)\). Unsupported lags are absent rather than coded as zero.

Every long-horizon coefficient reports observations, products, partners, clusters, and support relative to event month zero. For staggered treatment, changing cohort composition is disclosed. Beyond the immediate event, estimates describe the realized 2025 tariff episode, including later policy modifications; they are not interpreted as the isolated effect of the February ten-percentage-point action.

## Reproduction commands

```powershell
.venv\Scripts\python.exe -m scr.data_construction.extension_2025 --inventory-only
.venv\Scripts\python.exe -m scr.data_construction.extension_2025
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --preflight-only
```

After the independent policy gate passes:

```powershell
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --run --horizon all
.venv\Scripts\python.exe -m scr.pass_through.extension_2025 --finalize-only
.venv\Scripts\python.exe master_pipeline.py --extension-2025 --estimate-extension-2025
```

The estimation and finalization commands intentionally fail while the policy manifest remains blocked. Consequently, no 2025 coefficient or figure is presented as an empirical result yet. This is a policy-data limitation, not a trade-data or horizon limitation.
