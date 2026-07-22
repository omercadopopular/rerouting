# Replication Methodology

## Event study

For outcome \(y_{ict}\), the locked event regression mirrors `fig_02_m_event.do`:

\[
100\log y_{ict}=\sum_{k=-5}^{6}\beta_k
\mathbf{1}\{T_i=1,\;event_{it}=k\}
+\sum_{k=-5}^{6}\gamma_k\mathbf{1}\{event_{it}=k\}
+\alpha_{ic}+\delta_{ct}+\eta_{it}+\varepsilon_{ict}.
\]

Month \(-6\) is the omitted baseline, and horizons at or above \(+6\) are top-coded in the published-window replication. Fixed effects are product-country (`id`), country-month (`ct`), and product-month (`ht`). Standard errors are clustered by HS8 and country. Treatment is the time-invariant maximum of the paper-status variable by product.

The separate long-horizon specification preserves the same baseline, controls, fixed effects, clustering, and treatment definition but top-codes the right tail at \(24+\). It is labeled separately and never substituted for the paper benchmark.

## Dynamic regression

Following `tab_04_sigma_omega.do`, define \(\Delta\ell y_{ict}\) as the first difference of the package-compatible log outcome and \(x_{ict}=\Delta\log(1+\tau_{ict})\). The regression is

\[
\Delta\ell y_{ict}=\sum_{k=1}^{6}\phi_{-k}F^kx_{ict}
+\phi_0x_{ict}
+\sum_{k=1}^{K}\phi_kL^kx_{ict}
+\text{missing-lead/lag indicators}
+\eta_{it}+\delta_{ct}+\kappa_{cs}+u_{ict},
\]

where \(K=6\) in the locked replication and \(K=24\) in the separate extension. Fixed effects are product-month, country-month, and country-NAICS4. Standard errors are clustered by HS8 and country. The plotted response at horizon \(h\) is the cumulative linear combination of the appropriate lead, contemporaneous, and lag coefficients; its standard error uses the full estimated covariance matrix.

## Interpretation of the three lines

- **Original regression** uses the authors’ package outcomes, sample, and policy variables. It establishes estimator fidelity and is compared to the published PDF.
- **Replication** uses raw Census CIF outcomes and the independently reconstructed MFN+201+232+301 policy under the paper-compatible clock.
- **Alternative timing** uses the same raw outcomes and independent tariff path but assigns event time by the legal-effective calendar.

The paper and legal dynamic lines use the same bilateral day-weighted tariff path; they differ in the event-clock interpretation where applicable. The package policy anchor is not independent tariff evidence.
