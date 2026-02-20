# Issue 5960 – Evidence: bug, fix, test tightening, loopholes

## 1. Evidence the bug existed

**Code path that causes the bug:**

| Location | Evidence |
|----------|----------|
| `ClusterCron.ts` L71 | `entityId = options.calculateNextRunFromPrevious ? "initial" : DateTime.formatIso(next)` → when true, first message uses entity_id `"initial"`. |
| `ClusterCron.ts` L133-134 | `CronPayload [PrimaryKey.symbol]() { return "" }` → primary key is always empty. |
| `Envelope.ts` L366 | `primaryKeyByAddress` → `message_id = \`${entityType}/${entityId}/${tag}/${id}\`` with `id = PrimaryKey.value(payload)` → for InitialRun, `id === ""` every time. |
| `SqlMessageStorage.ts` | Table has `UNIQUE (message_id)` and insert uses `ON CONFLICT (message_id) DO NOTHING` (or dialect equivalent). |

**Conclusion:** For every restart, InitialRun sends a message with the same `message_id` (same entityType, entityId `"initial"`, tag, and empty id). The second insert is skipped, so no new message is stored and the cron chain never restarts after a pod restart.

---

## 2. Evidence the correct fix solves it

**Intended fix:** In `ClusterCron.ts`, make the payload’s primary key unique per run (e.g. `return crypto.randomUUID()` in `CronPayload [PrimaryKey.symbol]()`). Keep `entityId = "initial"` when `calculateNextRunFromPrevious` is true.

**Why that fixes it:**

- `message_id` includes `id = PrimaryKey.value(payload)`.
- With a unique value per run, the second InitialRun message gets a different `message_id`.
- The second insert is no longer a duplicate, so it is accepted and the chain restarts.

**Evidence in the test:** With the fix applied, the test sees `COUNT(DISTINCT message_id) WHERE entity_id='initial' >= 2`, i.e. both runs’ InitialRun messages were stored.

---

## 3. Loopholes considered and how the test closes them

| Loophole | How test rejects it |
|----------|----------------------|
| **Remove entityId="initial"** (use `DateTime.formatIso(next)` for first message too) | Assert at least one row with `entity_id = 'initial'`. With that workaround there are no such rows. |
| **Relax dedup in storage** (e.g. skip UNIQUE or allow duplicate message_id for "initial") | Assert `COUNT(DISTINCT message_id) WHERE entity_id='initial' >= 2`. With the bug, only one message_id exists (second insert was rejected). So we require two *distinct* message_ids, which only happens if ClusterCron sends a new primary key (the real fix), not if storage just allows duplicates. |
| **Run 2 only processes run 1’s leftovers** (run 1 never executes) | Assert `afterFirst >= 1` so run 1 is required to have executed at least once. |
| **Truncate or use a different DB between runs** | Test uses the same `dbPath` for both runs and does not truncate; comment states “same DB, no truncate”. |
| **Use in-memory or non-SQL storage** | Test uses `SqlMessageStorage.layer` and SQLite so UNIQUE and ON CONFLICT apply. |

---

## 4. Evidence the test is tightened and will catch the bug

**Assertions in order:**

1. **afterFirst >= 1** – Run 1 executed at least once (no “run 2 only” pass).
2. **afterSecond > afterFirst** – Run 2 contributed at least one execution (restart works).
3. **Rows with entity_id = 'initial'** – The “initial” path is still used (rejects “remove initial” workaround).
4. **COUNT(DISTINCT message_id) WHERE entity_id='initial' >= 2** – Both runs’ InitialRun messages were accepted, with two different message_ids (rejects storage-only workarounds; only a unique PrimaryKey in ClusterCron can satisfy this with `entity_id='initial'`).

**With the bug present:**

- Second InitialRun insert is skipped → only one row with `entity_id='initial'` and one distinct `message_id`.
- So assertion 4 fails (`distinctMessageIds >= 2`). The test fails and the bug is detected.

**With the correct fix:**

- Both inserts succeed with different `message_id` values.
- All four assertions pass.

So the test is tightened and will fail when the bug is present and pass only when the intended fix (unique PrimaryKey, keep entityId="initial") is in place.
