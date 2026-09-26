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
decisions are made on completed candles and fills happen at the next open:

- **`signal/`** - runs **early, before the open**, on the previous session's
  completed daily candles. Analysis only, never places an order. It does two
  things, on **two different clocks**:
  - **every session** - update each holding's trailing peak and flag stop
    breaches. A stop can break on any session, so this is unconditional. It
    needs candles for the holdings only (at most 4).
  - **only when a rebalance is due** (`month_key()` differs from the
    `last_rebalanced_month` that `momentum` stamps) - rank the full universe and
    write the monthly-rebalance plan to Mongo. The rank is read at exactly one
    moment, that rebalance, for the retain band and the new entries; on the
    other ~20 sessions of a month computing it would cost 28 rate-limited Dhan
    calls to produce a number nothing consumes. Skipping it changes **no**
    decision the strategy makes - the only visible difference is that the daily
    Slack ranking post now appears on rebalance days only.

  Because `last_rebalanced_month` is stamped by `momentum` when it *completes* a
  rebalance, the condition stays true until the plan has actually been consumed:
  if `momentum` misses the 1st, `signal` re-ranks and refreshes the plan every
  session until it doesn't - which is also what keeps the plan inside
  `momentum`'s `PLAN_MAX_AGE_DAYS` window.

  *(It must not act on a session before that session's daily bar is published.
  Rather than trust the cron slot for that, the job asks **one** liquid name -
  `NIFTYBEES`, the `CALENDAR_REF` constant - for the newest daily bar and
  compares it with the `last_candle_date` it stored last run; if they match
  there is nothing new to decide and it stops, one Dhan call in. Which session
  is "now" is a property of the market, not of what we happen to hold, so this
  works with zero holdings and on a month with no rebalance due. It also makes
  the job safe to re-run by hand, and logs the candle date next to the wall
  clock every run - so the Slack history measures Dhan's real publish lag for
  free. How soon after the close Dhan publishes has not been measured; the only
  observation is that the 2026-09-25 bar was absent at 22:57 and present by
  12:03 the next day, a 13-hour bracket that proves nothing narrower.)*
- **`momentum/`** - runs **at/just after the open**. The only job that trades, and
  it is deliberately the simpler of the two: flag the rotations a rebalance calls
  for, sell everything flagged (stops and rotations share one flag and one loop),
  and then either stop - because anything sold today leaves the cash unsettled -
  or buy the plan's new entries. See *Settlement* below.

`momentum` only runs a rebalance if the stored plan was generated by `signal`
within the last few days, so a missed `signal` run can never fire a stale
rebalance. Each folder is a standalone job (own `Dockerfile` + `requirements.txt`
+ `.env`), exactly like the other strategies.

## Settlement: T+1 (sell today, come back tomorrow)

The broker (Dhan) runs a **strict T+1 settlement cycle** - cash freed by a
same-day **sell** is **not usable to buy** until the next session. `momentum`
handles this by not trying to be clever about it:

```
1. rebalance due + fresh plan?  -> flag every holding the plan did not retain
2. sell everything flagged marked_for_exit
3. sold anything?               -> STOP. Nothing else can happen today.
4. otherwise                    -> buy the plan's new entries, THEN stamp the month
```

The job never remembers what to buy tomorrow, because **the plan already is that
memory**. What brings it back is that `last_rebalanced_month` is stamped in step
4 and nowhere else: until the buys are placed the rebalance is still "due", so
`signal` keeps the plan refreshed and `momentum` keeps trying.

So a rebalance that sells splits naturally across two sessions:

- **Day 1** - rotations are flagged and sold, the job stops, the month stays
  unstamped.
- **Day 2** - everything not retained is already gone, so step 1 finds nothing to
  mark and step 2 sells nothing. Step 4 buys the new entries with the
  now-settled cash and stamps the month. This matches the `entry_lag=1` backtest.

Three cases fall out of that ordering rather than needing their own code:

- **A rebalance that sells nothing** (the very first run, or a month whose
  holdings were all retained) skips straight to step 4 and buys the **same day**.
- **A mid-month trailing stop** sells and stops. There is no rebalance due, so
  nothing is bought - which is exactly the no-refill-on-stop rule.
- **A rejected order** retries itself. A rejected *sell* stays flagged and is
  picked up by the next session's step 2; a rejected *buy* leaves the month
  unstamped, so the next session tries again. Neither needs a recovery path.

Deferring (only when something was sold) costs roughly **~1.8 CAGR points** vs a
hypothetical fully same-day rotation - the freed sale cash sits idle one extra day
and a next-session entry buys slightly higher. It is a one-time step per rotation,
not a per-day bleed; buys funded by already-settled cash (first run / idle
redeploys) are **not** delayed.

> **One consequence worth knowing.** Because day 2 re-reads the plan rather than a
> frozen shopping list, it buys against whatever ranking `signal` wrote that
> morning - fresher, and computed from the holdings that actually survived. In a
> tightly bunched field a name rotated out on day 1 could in principle re-enter on
> day 2. It needs a jump from outside the retain band into the top few in a single
> session of 21-day momentum, so it is rare; the cost if it happens is one
> round-trip of brokerage, which is cheaper than the bookkeeping that would
> prevent it.

## MongoDB schema (`Bots` database)

Shared, read by both jobs (seeded once; already populated in prod):

| Collection | `_id` | Contents |
|------------|-------|----------|
| `etf_universe` | symbol | `{symbol, tsym, secid, bucket, name, is_park}` - 27 ETFs + `LIQUIDCASE` |

This is now the **only** shared collection. The strategy parameters are constants
in the job files (see *The strategy* above), so there is nothing else to seed.

Per-user ledger (`<user>` = `user_name` env; created lazily by `momentum`):

| Collection | Docs |
|------------|------|
| `etf_positions_<user>` | one doc per position, and **nothing else** |
| `etf_state_<user>` | the three control singletons - `_id:accounts` (the ledger), `_id:meta` (`last_candle_date`, `last_rebalanced_month`), `_id:plan` (the rebalance plan, which doubles as the T+1 memory of what to buy) |
| `etf_orders_<user>` | one doc per placed/simulated order |

The control docs used to share the positions collection, which worked only because
they had no `status` field and so fell out of `find({"status": "active"})`. That is
a trap now that the ledger is **derived** from exactly those queries - one stray
`status` field on a singleton would silently skew the P&L - so they live apart.

`_id:accounts` is a **cache**: `momentum` recomputes it from the position documents
after every buy and sell (`compute_accounts`), so it self-heals and can never drift.
Read it, don't write it.

## Layout

```
momentum-investing/
|-- signal/
|   |-- momentum_signal.py   # morning analysis job (stops daily, ranking + plan monthly, no orders)
|   |-- requirements.txt     # all deps (incl. tamingnifty==2.1.0)
|   |-- Dockerfile           # COPY . ; pip install -r src/requirements.txt
|   `-- .env                 # secrets (gitignored)
|-- momentum/
|   |-- momentum.py          # morning order job (DRY-RUN unless live_trading=true)
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
the plan and the stop flags that `signal` writes.

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
  and places them the next session. *(That replay predates the 2026-09-26 flow
  simplification, which removed the `pending_buys` snapshot; the same day-1/day-2
  split is now produced by the stop-and-return ordering instead, and is covered by
  a 26-check branch test.)* The
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
