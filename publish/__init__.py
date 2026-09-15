"""The boundary between this repo and everyone downstream of it.

Two modules that produce something another person reads:

* `build_report.py` — the Evidence site: the npm commands in the order the
  dashboard needs them, wrapped so `just report` and the `reports/evidence_site`
  asset cannot run different builds.
* `bus_matrix.py` — the conformed-dimension matrix in `docs/WAREHOUSE.md`,
  derived from the dbt manifest and never written by hand.

A layer rather than a directory of helpers, because what leaves the project is
where a policy applies: what may be published, under what licence, with which
columns named. A project that publishes its warehouse adds that module here, and
`scripts/` stays for what is genuinely one-off.
"""
