/**
 * ClusterCron tests.
 *
 * Issue 5960: When using ClusterCron with SQL storage and calculateNextRunFromPrevious,
 * the InitialRun singleton sends a message with entity_id="initial" and a deterministic
 * message_id (CronPayload has empty PrimaryKey). On pod restart, the second InitialRun
 * message is rejected by the UNIQUE constraint (ON CONFLICT DO NOTHING), so the cron
 * chain never restarts. This test asserts that after simulating a restart (two separate
 * layer runs with the same SQL DB), at least one execution occurs in the second run.
 *
 * Evidence the test validates the fix:
 * - Uses SQL storage (SqlMessageStorage) so UNIQUE on message_id and ON CONFLICT DO NOTHING apply.
 * - Uses calculateNextRunFromPrevious: true so InitialRun uses entity_id="initial" and the buggy path.
 * - First run: cluster starts, InitialRun sends message, we advance time so it is polled and executed.
 * - Second run: same DB (no truncate), cluster "restarts"; InitialRun runs again.
 * - If bug present: second InitialRun message is duplicate → rejected → no new message → no execution in run 2.
 * - If fixed: second InitialRun message has unique message_id → accepted → chain continues → execution in run 2.
 * - We assert runCount after second run > runCount after first run.
 *
 * Before the fix (entity_id="initial" + empty PrimaryKey → same message_id),
 * the second run's InitialRun message is rejected by SQL UNIQUE; the cron chain
 * does not restart, so no execution in run 2 and the assertion fails.
 */
import {
  ClusterCron,
  RunnerHealth,
  RunnerStorage,
  Runners,
  Sharding,
  ShardingConfig,
  Snowflake,
  SqlMessageStorage
} from "@effect/cluster"
import { FileSystem } from "@effect/platform"
import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { describe, expect, it } from "@effect/vitest"
import { Cron, Effect, Either, Layer, Ref, TestClock } from "effect"

const TestCronConfig = ShardingConfig.layer({
  entityMailboxCapacity: 10,
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 1500,
  sendRetryInterval: 100
})

describe("ClusterCron", () => {
  it.scoped("InitialRun survives pod restart with SQL storage (Issue 5960)", () =>
    Effect.gen(function*() {
      const runCount = yield* Ref.make(0)
      const fs = yield* FileSystem.FileSystem
      const tmpDir = yield* fs.makeTempDirectoryScoped()
      const dbPath = tmpDir + "/cluster.db"
      const sqliteLayer = SqliteClient.layer({ filename: dbPath })

      const storageLayer = SqlMessageStorage.layer.pipe(
        Layer.provideMerge(Snowflake.layerGenerator),
        Layer.provide(TestCronConfig),
        Layer.provide(sqliteLayer)
      )

      const cron = Cron.parse("*/1 * * * * *").pipe(Either.getOrThrow)
      const cronLayer = ClusterCron.make({
        name: "Issue5960",
        cron,
        calculateNextRunFromPrevious: true,
        execute: Ref.update(runCount, (n) => n + 1)
      })

      const shardingLayer = Sharding.layer.pipe(
        Layer.provide(Runners.layerNoop),
        Layer.provide(RunnerStorage.layerMemory),
        Layer.provide(RunnerHealth.layerNoop),
        Layer.provide(storageLayer),
        Layer.provide(TestCronConfig)
      )
      const fullLayer = cronLayer.pipe(Layer.provide(shardingLayer))
      // First "process" run: start cluster, let InitialRun send and get processed.
      // Effect.provide merges layer context with current (test) context, so TestClock is preserved.
      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)

      const afterFirst = yield* Ref.get(runCount)

      // Second "process" run: same DB, no truncate — simulates pod restart
      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)

      const afterSecond = yield* Ref.get(runCount)
      expect(afterSecond).toBeGreaterThan(afterFirst)
    }).pipe(
      Effect.provide(NodeFileSystem.layer)
    ))
})
