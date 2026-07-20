# Pooled policy validation status

The current source-only build is a diagnostic, not a release artifact.  The
validator joins the independently built panel to the corrected package cache
only for measurement.  It reports treatment assignment, legal/paper timing,
additional-rate differences, day-weighted differences, and family-level
coverage.  Total statutory rates are not compared until raw base statutory
scope is resolved; Chapter-99 sentinel values such as `9999.99` are treated as
missing rather than a rate.

Current diagnostic output from the local run:

- 4,199,002 common package rows;
- treatment assignment match: 0.977965;
- trade-weighted treatment match: 0.969422;
- additional-rate MAE against the package tariff field: 0.033085;
- day-weighted additional-rate MAE: 0.033075;
- exact effective-month match: 0.237831;
- paper-month within-one match: 0.482849.

These values are not a passed independent-policy gate.  The principal reason
is incomplete source scope for the positive 232 rules and solar family, plus
the fact that the package comparison field is not a substitute for a legal
base-rate reconstruction.  The historical package-only replication remains a
separate passed estimator/PDF result.  Section 301 v5 and the 2025 policy/event
study remain blocked.
