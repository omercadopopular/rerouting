# Tariff-Data Construction

## Statutory rate

For partner \(c\), product \(i\), and month \(t\), the independent total tariff is

\[
\tau_{ict}=\tau^{MFN}_{it}
+\Delta\tau^{201}_{ict}
+\Delta\tau^{232}_{ict}
+\Delta\tau^{301}_{ict}.
\]

The policy database retains each component, its official source, legal effective date, paper-compatible event month, and product/partner applicability. Rates that cannot be established from a source are left missing; they are never replaced with zero merely to complete a panel.

## Day weighting

Suppose an additional rate \(\Delta\tau\) becomes effective on day \(d\) of a month with \(D\) days. We follow the arithmetic implied by “days in effect.” With the legal effective date treated as the first active day boundary, the initial-month contribution is

\[
\Delta\tau^{dw}_{t}=\Delta\tau\frac{D-d}{D},
\]

and the balance carried into the following monthly change is

\[
\Delta\tau-\Delta\tau^{dw}_{t}=\Delta\tau\frac{d}{D}.
\]

Full subsequent months receive the full statutory rate. This convention is recorded explicitly because prose examples in the replication documentation can otherwise be read in two ways.

## Two timing conventions

The paper-compatible event clock assigns event time zero to the legal month when the action takes effect on or before the fifteenth. An action after the fifteenth is assigned to the following month:

\[
E^{paper}(d)=
\begin{cases}
t,&d\leq 15,\\
t+1,&d>15.
\end{cases}
\]

The legal clock assigns event zero to the calendar month containing the legal effective date. Only the paper clock is compared to the original paper. The legal clock is reported as an alternative-timing diagnostic.

For products without their own event date, the Stata design assigns the earliest event month observed among products in the same NAICS4 industry. If that is unavailable it tries NAICS3 and then NAICS2; February 2018 is the final fallback. This does not label an untreated product as treated. It supplies a common event-time origin needed to retain controls in a staggered event study.

## Dynamic tariff changes

The dynamic design uses the first difference

\[
x_{ict}=\Delta\log(1+\tau_{ict}),
\]

with exact calendar leads and lags. Exact means that \(F^kx_{ict}\) or \(L^kx_{ict}\) is missing when month \(t+k\) or \(t-k\) is absent; rows are not shifted across calendar gaps.

## Scope and exclusions

The locked full panel contains MFN, Section 201, Section 232, and Section 301 components. It excludes AD/CVD, unrelated treaty changes, and quota-threshold-only increments, matching the scope described in the paper appendix. For the separate post-2019 horizon exercise, the validated tariff level is frozen at April 2019. This is a transparent no-new-actions convention, not a reconstruction of the later trade war.
