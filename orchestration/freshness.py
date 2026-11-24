"""The freshness policies the assets in `orchestration/defs/` share.

Outside `defs/` because they are not definitions: every asset, spec and check a
module in there holds at module level is registered, and a helper is safest
where nothing loads. They state what *should* be true whether or not a run
happened, so a schedule that quietly stopped firing shows as a stale asset in
the UI instead of having to be inferred from an absent run.
"""

from __future__ import annotations

from datetime import timedelta

import dagster as dg

# Raw pulls: publishers push on their own cadence, so two days without a
# successful load is worth a warning and a week is a failure.
RAW_FRESHNESS = dg.FreshnessPolicy.time_window(
    fail_window=timedelta(days=7),
    warn_window=timedelta(days=2),
)

# Modelled layers hang off the daily 06:00 schedule: rebuilt by 08:00 UTC from
# data no older than the preceding midnight.
MODELLED_FRESHNESS = dg.FreshnessPolicy.cron(
    deadline_cron="0 8 * * *",
    lower_bound_delta=timedelta(hours=8),
)
