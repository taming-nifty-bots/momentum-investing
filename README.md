# ETF Momentum Rotation - Live (Dhan)

A live-deployable port of the validated **"Final" ETF Momentum Rotation strategy**
(backtest config `21 / 4 / 7`, trailing 8% stop, no mid-month refill). Same stack
as the author's other live strategies: **Dhan via the
`tamingnifty` package** with **MongoDB** as the ledger, split into two small,
self-contained, scheduled container jobs.

> **Not yet cleared for real money.** This strategy has zero forward-test credit.
> The 100-trade rule applies: it must forward-test in **DRY RUN** (`live_trading=false`)
> before `live_trading=true` is ever set. See *Safety* below.

## The strategy (exact rules)

Every number below is a 1:1 transcription of the validated engine (`bt_v2.py`,
`FINALCFG`) and lives as a **constant at the top of the job file** -
`LOOKBACK / N_HOLD / TOP_RETAIN / STOP_PCT / MOMENTUM_MIN` in
`signal/momentum_signal.py`, `START_CAPITAL` in `momentum/momentum.py`. They used
to sit in an `etf_params` document in Mongo, which meant the strategy could be
changed without a code change and without leaving a trace; as constants, git
history is the audit trail and changing one costs a commit and a redeploy. The
universe stays in Mongo - that is data, not a rule.

| Rule | Value |
|------|-------|
| Universe | 27 curated, liquidity-screened NSE ETFs (+ one liquid-cash park) |
| Momentum | close-to-close return over **21 trading days** (candles, not calendar) |
| Eligibility | momentum **strictly > 0**, else not rankable (no SMA regime gate) |
| Holdings | **4**, equal-weight of *freed cash* on entry |
| Retention band | keep a holding while its rank <= **7** (`top_retain`) |
| Bucket cap | at most **one** holding per asset-class bucket |
| Rebalance | **monthly**, first trading session of each month |
| Settlement | **T+1** - if anything sold today, the job stops; the buys land the next session on settled cash |
| Exit (daily) | **trailing stop 8%** below the peak *daily close* (ratchets up only) |
| Fills | every signal is identified on the **close**, filled at the **next open** |
| Mid-month refill | **NONE** - cash freed by a stop waits for the next rebalance |

### Built-in concentration risk (intended, faithful behaviour)

New entries are funded **only** from cash freed at a rebalance (rotate-out
proceeds + idle cash). Retained winners are **never topped up or trimmed**
("let winners run"). A single runaway winner can therefore grow toward ~100% of
the book, and when it leaves ~0 free cash the rebalance buys nothing new. This
concentration partly *drove* the backtest's high CAGR and is a real single-name
risk - the live jobs keep the cash gate exactly, because that IS the validated
strategy. Manage it with **capital size**, not code.

## Architecture: two jobs

The backtest processes each day as **(1) apply stop exits at the open -> (2) on a
month boundary, rebalance**. Live, that is split into two scheduled jobs so
decisions are made on completed candles and fills happen at the next open.

The split is not an even one, and deliberately so: **`signal` decides everything,
`momentum` decides nothing.** They never call each other - they meet at exactly
two fields in Mongo:

| Field | Written by | Read by |
|-------|-----------|---------|
| `marked_for_exit` on a position | `signal` | `momentum` sells it, whatever the reason |
| `plan.target` | `signal` | `momentum` buys whatever of it is missing |

Each target entry carries everything needed to place the order -
`{symbol, secid, tsym, bucket}` - so `momentum` never reads `etf_universe` at
all. It has to come from `signal`: a name we do not hold yet has no position
document to look it up on, and `signal` has already resolved all three fields at
the moment it writes the plan.

- **`signal/`** - runs **early, before the open**, on the previous session's
  completed daily candles. Analysis only, never places an order. It does two
  things, on **two different clocks**:
  - **every session** - update each holding's trailing peak and flag stop
    breaches (`marked_for_exit`, `exit_reason: trailing_stop`), and remove the
    stopped name from the target. A stop can break on any session, so this is
    unconditional. It needs candles for the holdings only (at most 4).
  - **only when a rebalance is due** (`month_key()` differs from the
    `last_rebalanced_month` it stamps itself) - rank the full universe, flag the
    holdings that fell out of the retain band (`marked_for_exit`,
    `exit_reason: rebalance`), write the **target** book, and stamp the month.
    The rank is read at exactly one moment, that rebalance; on the other ~20
    sessions of a month computing it would cost 28 rate-limited Dhan calls to
    produce a number nothing consumes. Skipping it changes **no** decision the
    strategy makes - the only visible difference is that the daily Slack ranking
    post now appears on rebalance days only.

  Rotations and stops carry the **same flag** on purpose. To `momentum` an exit
  is an exit; `exit_reason` is a label for the ledger, not something it acts on.

  *(Every run asks **one** liquid name - `NIFTYBEES`, the `CALENDAR_REF`
  constant - for the newest daily bar, and stamps that date on the plan and on
  `meta.last_candle_date`. It is a **record, not a gate**: the job does not stop
  when the bar is unchanged since the last run, it just says so in Slack next to
  the wall clock. Deciding whether today's bar is published yet is done by
  choosing the cron slot, by hand. Which session is "now" is a property of the
  market, not of what we happen to hold, so this works with zero holdings and on
  a month with no rebalance due. How soon after the close Dhan publishes has not
  been measured; the only observation is that the 2026-09-25 bar was absent at
  22:57 and present by 12:03 the next day, a 13-hour bracket that proves nothing
  narrower - which is why the log line is there.*

  *Re-running the job by hand is still harmless, but that is the doing of
  `last_rebalanced_month`, not of the candle date: a second run on the same month
  redoes the stop maintenance, which is idempotent, and skips the ranking
  entirely. The risk the candle date no longer covers is running on a day when
  Dhan has not published yet - then a rebalance ranks on the previous session's
  close.)*
- **`momentum/`** - runs **at/just after the open**. The only job that trades,
  and it is deliberately trivial: sell everything flagged, and if nothing sold,
  buy whatever the target says is missing. It holds no month logic, no ranking,
  and no notion of what a "rebalance" is. See *Settlement* below.

Each folder is a standalone job (own `Dockerfile` + `requirements.txt` + `.env`),
exactly like the other strategies.

## Settlement: T+1 (sell today, come back tomorrow)

The broker (Dhan) runs a **strict T+1 settlement cycle** - cash freed by a
same-day **sell** is **not usable to buy** until the next session. `momentum`
handles this by not trying to be clever about it:

```
1. sell every holding flagged marked_for_exit
2. sold anything?  -> STOP. Nothing else can happen today.
3. otherwise       -> buy whatever of plan.target we are not already holding
```

The job never remembers what to buy tomorrow, because **the target is not a
shopping list - it is the book we should end up holding**. Tomorrow it compares
the holdings against the target again and the missing names are still missing.
That one property is what makes the whole thing work:

- **re-running is harmless** - reconciling twice changes nothing;
- **a missed `momentum` run** is simply picked up by the next session;
- **a missed `signal` run** leaves the target as it was, so nothing unexpected
  is bought;
- **every failure retries itself.** A rejected *sell* stays flagged and is sold
  next session. A rejected *buy* leaves the name missing, so the next session
  buys it. Neither needs a recovery path.

So a rebalance that sells splits naturally across two sessions:

- **Day 1** - `signal` has flagged the rotations; `momentum` sells them and stops.
- **Day 2** - nothing is flagged, so nothing sells, and the names still missing
  from the target are bought with the now-settled cash. This matches the
  `entry_lag=1` backtest.

Two cases fall out of that ordering rather than needing their own code:

- **A rebalance that sells nothing** (the very first run, or a month whose
  holdings were all retained) skips step 2 and buys the **same day**.
- **A mid-month trailing stop** sells and stops, and buys nothing back - because
  `signal` removed the stopped name from the target when it flagged it. That one
  line *is* the no-refill-on-stop rule; leave it out and the next session would
  buy the name straight back.

Deferring (only when something was sold) costs roughly **~1.8 CAGR points** vs a
hypothetical fully same-day rotation - the freed sale cash sits idle one extra day
and a next-session entry buys slightly higher. It is a one-time step per rotation,
not a per-day bleed; buys funded by already-settled cash (first run / idle
redeploys) are **not** delayed.

## MongoDB schema (`Bots` database)

Shared, read by both jobs (seeded once; already populated in prod):

| Collection | `_id` | Contents |
|------------|-------|----------|
| `etf_universe` | symbol | `{symbol, tsym, secid, bucket, name, is_park}` - 27 ETFs + `LIQUIDCASE` |

Read by `signal` only. `momentum` reads no shared collection - everything it
needs to place an order reaches it on a position document or a target entry. The strategy parameters are constants
in the job files (see *The strategy* above), so there is nothing else to seed.

Per-user ledger (`<user>` = `user_name` env; created lazily by `momentum`):

| Collection | Docs |
|------------|------|
| `etf_positions_<user>` | one doc per position, and **nothing else** |
| `etf_state_<user>` | the three control singletons - `_id:accounts` (the ledger), `_id:meta` (`last_candle_date`, `last_rebalanced_month`, both written by `signal`), `_id:plan` (four fields, see below) |
| `etf_orders_<user>` | one doc per placed/simulated order |

`_id:plan` is deliberately four fields and nothing more:

| Field | What it is |
|-------|-----------|
| `for_month` | the month this rebalance is for, e.g. `2026-10` |
| `signal_date` | the daily candle it was decided on |
| `target` | the book `momentum` reconciles towards - `{symbol, secid, tsym, bucket}` per name |
| `ranking` | every eligible ETF that session: `{symbol, rank, momentum}` |

`target` is the only field `momentum` reads. What was retained, what rotated out
and what got stopped is **not** stored here, because the position documents
already record it (`entry_date`, `status`, `exit_reason`) - a copy in the plan
would only go stale the moment a mid-month stop edits `target`. `ranking` stays
because it is the one thing the positions cannot tell you after the fact: why
these four names and not the others. Same reasoning as `_id:accounts` being
derived rather than kept by hand.

The control docs used to share the positions collection, which worked only because
they had no `status` field and so fell out of `find({"status": "active"})`. That is
a trap now that the ledger is **derived** from exactly those queries - one stray
`status` field on a singleton would silently skew the P&L - so they live apart.

`_id:accounts` is a **cache**: `momentum` recomputes it from the position documents
after every buy and sell (`compute_accounts`), so it self-heals and can never drift.
Read it, don't write it.

## Implementation notes

Small operational facts that the code assumes. They live here rather than as
comments so the jobs stay short enough to read in one screen.

**Dhan rate-limits the data API.** Walking all 28 ETFs back to back with no pause
gets most calls refused with `DH-904`; measured 2026-09-25, **17 of 27 failed at
no sleep and 0 failed at 0.25s**. `PAUSE_BETWEEN_CALLS = 0.5` in
`momentum_signal.py` waits before every candle request - at half a second the
whole universe still finishes in under 20 seconds. This is the reason the
ranking is monthly rather than daily: an ordinary session costs ~4 calls
(reference + holdings) instead of ~29.

**`signal` fetches in stages and never fetches the same name twice** - the
calendar reference first, then the holdings, then (only on a rebalance) the rest
of the universe, each stage skipping what an earlier one already pulled. A
holding that has dropped out of `etf_universe` carries its own `secid` on the
position document, so its stop keeps being maintained.

**`secid` is the NSE exchange token, which is also Dhan's `securityId`** for cash
equity - the universe needed no change for the broker move. Dhan keys orders and
quotes by `securityId`, not by trading symbol; the `tsym` is carried alongside
purely so Slack messages and Mongo docs are readable. Every `secid` reaches
`momentum` already resolved - on the position document for a sell, on the target
entry for a buy - so the job that places orders never looks one up.

**Dhan only accepts orders from a whitelisted static IP.** `momentum` prints the
address the container actually egresses on at every run - that is the first thing
to check when orders start getting refused.

**A broker rejection is an expected outcome, not a crash.** Dhan answers a bad
order with an HTTP error which the library raises; `place_market()` turns it back
into a `{"orderStatus": "REJECTED"}` dict so the caller can inspect it and the
job survives. Terminal success on Dhan is `TRADED`, not `COMPLETE`.

**The strategy parameters are constants, not config.** They used to sit in an
`etf_params` document in Mongo, which meant the strategy could be changed without
a code change and without leaving a trace. As constants, git history is the audit
trail and changing one costs a commit and a redeploy - the right amount of
friction for numbers that define the strategy. The universe stays in Mongo
because it is data, and it is edited far more often.

## Layout

```
momentum-investing/
|-- signal/
|   |-- momentum_signal.py   # ALL decisions: stops daily, ranking + rotations + target monthly. No orders.
|   |-- requirements.txt     # all deps (incl. tamingnifty==2.1.0)
|   |-- Dockerfile           # COPY . ; pip install -r src/requirements.txt
|   `-- .env                 # secrets (gitignored)
|-- momentum/
|   |-- momentum.py          # NO decisions: sell what is flagged, buy what the target lacks (DRY-RUN unless live_trading=true)
|   |-- requirements.txt
|   |-- Dockerfile
|   `-- .env                 # secrets (gitignored)
`-- .github/workflows/       # CI/CD: build per-folder images, deploy 2 jobs
```

All job dependencies are installed **from `requirements.txt` inside the
Dockerfile** - no ad-hoc `pip install`, matching the other strategies. The
strategy parameters are constants at the top of each job file (no `config.py` in
the repo); the ETF universe lives in Mongo - edit it directly in the `Bots` DB.

## Running

```bash
# SIGNAL - early morning (writes peaks, stop flags, and the rebalance plan)
docker build -t etf-signal ./signal
docker run --rm --env-file signal/.env etf-signal

# MOMENTUM - at/just after the open (DRY RUN while live_trading=false)
docker build -t etf-momentum ./momentum
docker run --rm --env-file momentum/.env etf-momentum
```

In production these are cron-scheduled Azure Container App Jobs (see the CI/CD
workflow): `signal` early in the morning, `momentum` shortly after the open,
Mon-Fri. `signal` **must** finish before `momentum` starts - `momentum` acts on
the target and the exit flags that `signal` writes, and decides nothing itself.

## Safety

- **`live_trading=false` (default) = DRY RUN.** `momentum` simulates fills at LTP,
  updates the Mongo ledger, and sends nothing to the broker. Set
  `live_trading=true` only after explicit go-live sign-off. Same flag name and
  same true/false values as the other strategies.
- **100-trade rule.** This strategy has not been forward-tested. Run it in dry run
  and collect >= 100 trades before considering real capital.
- **Concentration.** See the runaway-winner caveat above - size total capital with
  the understanding that a single ETF can dominate the book.

## Validation status

- **Decision logic** is proven equivalent to the validated engine: the ranking +
  trailing-stop math in `signal/momentum_signal.py` matches the reference engine
  across 144,000 randomized checks (0 mismatches), and the earlier offline test
  matched `bt_v2.py` on 37/37 monthly rebalances and every stop breach date.
- **T+1 settlement (exit-d1 / enter-d2)** is validated end-to-end: a 3-year
  day-by-day replay drives the **real** `signal` + `momentum` modules (over a fake
  Mongo + mock broker) and compares to the `entry_lag=1` backtest. Every rebalance
  correctly defers its buys (no buy ever lands on a session that sold something)
  and places them the next session. *(That replay predates the 2026-09-26 rework, which moved every decision into
  `signal` and left `momentum` reconciling holdings against `plan.target`; the
  same day-1/day-2 split is now produced by the sell-then-stop ordering instead,
  and is covered by a 26-check branch test on `momentum`, plus a 22-check test
  that `signal` really does make every decision - including that a stop removes
  its name from the target, which is the no-refill rule.)* The
  port reproduces the engine's rebalance decisions except for a handful of
  marginal whole-unit buys over 3 years (benign, +2.25% on the ledger). T+1 costs
  ~3 CAGR points vs same-day (engine 55.9% vs 58.9%).
  *(That replay ran with a 5 bps sizing buffer that has since been removed, so
  whole-unit rounding is now the only source of divergence. Removing the buffer
  can only raise a buy by `alloc x 0.0005 / price` units — under 1 unit for any
  ETF above ~Rs50 at a ~Rs1L slice — so the conclusion stands, but the +2.25%
  figure itself has not been re-measured.)*
- **Symbols**: all 28 stored `secid` values are NSE exchange tokens, verified
  against Dhan's NSE cash scrip master on 2026-09-25 — every one matches Dhan's
  `SECURITY_ID` exactly (0 mismatch, 0 missing), so the universe needed no
  change for the broker move.
- **Not** yet validated: live broker fills, slippage, real-world execution - those
  only come from the dry-run forward test.
