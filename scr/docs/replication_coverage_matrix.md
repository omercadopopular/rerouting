# Replication coverage matrix

Status reflects empirical evidence available on 2026-07-17. Synthetic tests are
not treated as replication evidence.

| Item | Original program | Source mode | Status | Latest result | Blocker |
|---|---|---|---|---|---|
| Figure 2 import event study | `fig_02_m_event.do` | `package_full_benchmark` | `gate_passed` | Four outcomes; 13 horizons each; max PDF difference 0.86862 | None for package gate |
| Figure 4a import dynamic study | `fig_04_dynamic.do`, `tab_04_sigma_omega.do` | `package_full_benchmark` | `gate_passed` | Four outcomes; 13 horizons each; max PDF difference 1.00962 | Dynamic-variable equivalence should remain documented |
| Figure 4b export dynamic study | `fig_04_dynamic.do` | `package_full_benchmark` | `blocked_missing_data` | Not attempted | Export Figure 4b is absent locally |
| Common-sample outcome bridge | Section 301 v5 design | `raw_outcomes_package_policy` | `gate_failed` | Historical natural-sample bridge fails several CI/distance metrics; aligned import-only v2 has 4,197,758 identical keys and is pending estimation | Outcome reconstruction and sample restrictions |
| Independent Section 301 policy map | Section 301 source programs | `fully_raw_policy` | `gate_failed` | Existing corrected China metrics remain outside tolerance | Legal/timing/rate discrepancies |
| 2013–2025 raw trade extension | Trade panel builders | extension v2 archive-native | `ran_no_gate` | 312/312 ZSTD Parquet partitions; imports 39,640,207 rows and exports 50,400,363 rows; ZIP/staging value, key, and quantity comparison passes; import duty fields preserved | Native HTS-vintage concordance and CPI real-value gates pending |
| February 2025 event study | Future design | 2025 policy ledger | `blocked_missing_data` | Not estimated | Versioned policy ledger incomplete |
