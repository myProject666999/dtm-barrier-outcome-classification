# Barrier outcome classification

`BranchBarrier.CallWithOutcome(tx, busiCall)` preserves `Call`'s transaction and
business-call behavior while returning a stable classification:

```go
outcome, err := bb.CallWithOutcome(tx, func(tx *sql.Tx) error {
    // ...
    return nil
})
```

`Call` delegates to it and keeps its historical signature and return values.

## Outcome tokens

`BarrierOutcome.String()` and the structured log field `result` use the same
lower-snake-case tokens:

| Constant | String | Meaning |
| --- | --- | --- |
| `BarrierOutcomeExecuted` / `Executed` | `executed` | `busiCall` was invoked. |
| `BarrierOutcomeEmptyCompensation` / `EmptyCompensation` | `empty_compensation` | Compensation arrived before its origin operation. |
| `BarrierOutcomeDuplicate` / `Duplicate` | `duplicate` | The existing current-operation row was written by this same operation. |
| `BarrierOutcomeSuspended` / `Suspended` | `suspended` | The current operation hit a row written by an earlier compensation / `QueryPrepared`. |
| `BarrierOutcomeUndetermined` / `Undetermined` | `undetermined` | The duplicate row exists, but its `reason` could not be queried. |
| `BarrierOutcomeUnknown` / `Unknown` | `unknown` | Zero/error path before a stable classification is available. |

## Arrival order and classification

All barrier IDs below are `01` unless a preset `BarrierID: 1` makes the next
formatted ID `02`.

| Arrival order | Outcomes in order | `busiCall` count | Distinguishing `reason` / result |
| --- | --- | ---: | --- |
| `cancel` → `try` | `empty_compensation` → `suspended` | 0 | Cancel writes origin `try` row with `reason=cancel`; later `try` reads that reason. |
| `try` → `cancel` → `try` | `executed` → `executed` → `duplicate` | 2 | Committed `try` row has `reason=try`; retry therefore reads its own operation name. |
| `compensate` first → `action` | `empty_compensation` → `suspended` | 0 | Compensation writes the origin `action` row with `reason=compensate`. |
| `rollback` first → `action` | `empty_compensation` → `suspended` | 0 | Rollback writes the origin `action` row with `reason=rollback`. |
| Compensation after its origin action, then same compensation retried | `executed` → `duplicate` | 1 | First compensation inserts its current row; retry has both affected rows equal to zero. |
| Compensation arrived first, then same compensation retried | `empty_compensation` → `duplicate` | 0 | First call has `originAffected > 0`; retry has `currentAffected == 0`. |
| `QueryPrepared` → msg `Call` | `suspended`, and `CallWithOutcome` returns `ErrDuplicated` | 0 | `QueryPrepared` writes `op=msg`, `barrier_id=01`, `reason=rollback`; `QueryPrepared` itself returns `ErrFailure` when it reads `rollback`. |
| Prior msg `DoAndSubmit` row → msg `Call` | first `executed`, then `duplicate` + `ErrDuplicated` | 1 | Existing msg row has `reason=msg`. |
| Interleaved failed `try`, blocked `cancel`, `try` retry | failed `try` business call → `empty_compensation` → `suspended` | 1 total | After the `try` transaction rolls back, the blocked cancel insert succeeds. Final rows are `try(reason=cancel)` and `cancel(reason=cancel)`. |
| Postgres `try` after `cancel`, reason select fails | `undetermined` + select error from `CallWithOutcome`; `Call` returns `nil` | 0 | This attempt inserted nothing; its transaction is rolled back. |

The reason lookup is performed only when:

```text
currentAffected == 0
and op not in {cancel, compensate, rollback}
and the current insert returned no error
```

Compensation operations do not need the lookup: their origin insert affected
row already distinguishes an empty compensation from a compensation retry.

## SQL statement counts

`newBarrierID` remains called exactly once per outer `Call` /
`CallWithOutcome`; `Call` does not increment the barrier ID again. A preset
`BarrierID: 1` therefore produces `"02"` in both insert arguments and the
reason lookup arguments. Two calls on one new barrier produce `"01"` then
`"02"`.

| Path | SQL statements sent to the database |
| --- | ---: |
| First `try` / `action` / other non-origin operation | 1 `INSERT`; the first `InsertBarrier` call has empty `originOp` and returns before sending SQL. |
| Empty compensation (`cancel` / `compensate` / `rollback` first) | 2 `INSERT`s, unchanged. |
| Compensation duplicate/retry | 2 `INSERT`s, unchanged; no reason `SELECT`. |
| Non-compensation duplicate or suspended | Same 1 `INSERT`, plus 1 reason `SELECT`. |
| Msg duplicate / suspended | Same 1 msg `INSERT`, plus 1 reason `SELECT`; then `ErrDuplicated`. |

The in-memory driver tests in
`client/dtmcli/barrieroutcometest/barrier_outcome_memory_test.go` count these
statements per transaction and also cover commit-vs-rollback unique-key lock
behavior.

## Why `Call` and `CallWithOutcome` invoke business calls the same number of times

`Call` contains no barrier classification or business invocation anymore. It
calls `CallWithOutcome` and only translates the new diagnostic
`Undetermined` error back to `Call`'s historical compatibility result:

- ordinary errors, `ErrDuplicated`, commit errors and panics propagate exactly through the same `dtmimp.DeferDo` rollback/commit path;
- `busiCall` is invoked only on the `executed` path, inside `CallWithOutcome`;
- every non-`executed` outcome skips `busiCall`;
- for generic `Undetermined`, `Call` returns `nil` as before;
- for the msg operation, `Call` preserves the historical `ErrDuplicated`.

Thus running the same arrival sequence through either entry point gives the
same business-call count. The test suite asserts this for
`cancel -> try` and `try -> cancel -> try`.

## Reason lookup failure and transaction handling

The preferred implementation does not use a savepoint. When the reason query
fails:

1. outcome is `undetermined`;
2. `CallWithOutcome` returns the original query error for diagnostics;
3. the named error causes `dtmimp.DeferDo` to roll back;
4. `Call` suppresses that newly introduced diagnostic error and preserves its old `nil` return for non-msg operations.

Rollback loses no barrier/business data from this invocation:

- lookup happens before `busiCall`, so the business callback has not run;
- every operation eligible for lookup has no origin operation in `originOp`, so the first `InsertBarrier` is an in-memory no-op because `originOp == ""`;
- the current `INSERT` is known to have affected zero rows before the lookup;
- therefore this transaction has no successful row writes to preserve. Existing rows belong to earlier transactions and remain visible after rollback.

For the Postgres `try` after `cancel` example, with a preset barrier ID of
`1`, the chosen statement sequence is:

```sql
-- originOp is empty: InsertBarrier returns here, no SQL statement.
INSERT INTO <barrier_table>(...) VALUES ($1,...,$6)
  ON CONFLICT ON CONSTRAINT uniq_barrier DO NOTHING;  -- affected = 0
SELECT reason FROM <barrier_table>
  WHERE gid=$1 AND branch_id=$2 AND op=$3 AND barrier_id=$4; -- error
ROLLBACK;
```

There is no `COMMIT` in this sequence.

A savepoint-based design that tried to preserve the old commit-with-empty-
transaction behavior would instead need:

```sql
INSERT ... ON CONFLICT ON CONSTRAINT uniq_barrier DO NOTHING; -- 0
SAVEPOINT dtm_barrier_reason;
SELECT reason ...;                                            -- error
ROLLBACK TO SAVEPOINT dtm_barrier_reason;
COMMIT;
```

That adds two transaction-control statements and requires separating the
diagnostic error returned to the caller from the error consumed by the
existing named-return/`DeferDo` commit decision. Returning the query error
through the current named error naturally selects rollback instead. This is
simpler and also avoids Postgres's aborted-transaction state.

For the pinned `github.com/lib/pq v1.10.7`, `(*conn).Commit` checks
`txnStatusInFailedTransaction`, internally rolls back and returns
`pq: Could not complete operation in a failed transaction`
(`pq.ErrInFailedTransaction`). Hence swallowing the select error and calling
the old commit path would indeed replace `Call`'s historical `nil` with a
commit error. Explicit rollback avoids that.

## `QueryPrepared` table-name conclusion

Conclusion: this change aligns `QueryPrepared` with `InsertBarrier` and the new
lookup by using:

```go
dtmimp.OrString(bb.BarrierTableName, dtmimp.BarrierTableName)
```

Callers that only set the global `dtmimp.BarrierTableName` see no change.
Callers that set the per-barrier `BranchBarrier.BarrierTableName` previously
inserted into their custom table but selected `reason` from the global table.
After this correction, `QueryPrepared` reads the same custom table it inserted
into.

This is an intentional bug fix with an observable deployment effect: a
custom-table deployment that previously read the wrong table and received
`sql.ErrNoRows` can now read the real `reason=rollback` row and receive
`dtmcli.ErrFailure`. If a release policy strictly forbids behavior changes in
`QueryPrepared`, the alternative is to leave that method on the global table
and track the mismatch separately; the new outcome lookup should still use the
`InsertBarrier` table rule.

## Database-specific constraint name

The Postgres insert template names `uniq_barrier` explicitly:

```sql
INSERT ... ON CONFLICT ON CONSTRAINT uniq_barrier DO NOTHING
```

That name matches `sqls/dtmcli.barrier.postgres.sql`. The MySQL DDL creates a
unique key on `(gid, branch_id, op, barrier_id)` but does not define a
constraint named `uniq_barrier`; MySQL uses `INSERT IGNORE` and does not name
the constraint in the insert template. Do not describe `uniq_barrier` as a
cross-database common constraint name.

## Redis and Mongo note

The SQL map contains `rollback -> action`; the Redis
`RedisCheckAdjustAmount` and Mongo `MongoCall` origin-operation maps currently
contain only `cancel -> try` and `compensate -> action`. That difference is
recorded here but intentionally not aligned by this SQL-barrier change. Redis
and Mongo barriers are out of scope.
