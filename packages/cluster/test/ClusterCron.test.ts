/**
 * ClusterCron tests.
 *
 * Issue 5960: When using ClusterCron with SQL storage and calculateNextRunFromPrevious,
 * the InitialRun singleton sends a message with entity_id="initial" and a deterministic
 * message_id (CronPayload had empty PrimaryKey). On pod restart, the second InitialRun
 * message was rejected by the UNIQUE constraint (ON CONFLICT DO NOTHING), so the cron
 * chain never restarted.
 *
 * --- EVIDENCE THE BUG EXISTED (code path) ---
 * - ClusterCron: entityId = options.calculateNextRunFromPrevious ? "initial" : DateTime.formatIso(next)
 * - CronPayload [PrimaryKey.symbol]() returned "" → same message_id for every InitialRun
 * - Envelope: message_id = `${entityType}/${entityId}/${tag}/${id}` → id="" for every InitialRun
 * - SqlMessageStorage: UNIQUE(message_id), ON CONFLICT DO NOTHING → second insert skipped
 *
 * --- FIX ---
 * CronPayload now has a unique `id` per run (Crypto.randomUUID()); [PrimaryKey.symbol]() returns this.id.
 * Without this fix, the two "restart" tests below fail: distinctMessageIds stays 1 (second insert skipped).
 *
 * --- WHAT EACH TEST PROVES ---
 * 1. "InitialRun survives pod restart" (2 runs, same DB): Proves the bug is fixed.
 *    - afterFirst >= 1, afterSecond > afterFirst: cron ran and second run contributed.
 *    - distinctMessageIds >= 2 for entity_id='initial': both InitialRun messages were stored (not deduplicated).
 *    - rows with entity_id='initial' >= 1: we still use the "initial" path (no workaround that removes it).
 *    On main (buggy code) this test fails: distinctMessageIds === 1.
 *
 * 2. "InitialRun survives multiple pod restarts" (3 runs): Same proof for 3 restarts; distinctMessageIds >= 3.
 *
 * 3. "runs with calculateNextRunFromPrevious false": Regression test only — does NOT exercise the bug.
 *    Ensures the non-initial path (entityId = DateTime.formatIso(next)) still works after adding the
 *    required `id` field to CronPayload. Would pass on main; keep so we don't break that path.
 */
import {
  ClusterCron,
  RunnerHealth,
  Runners,
  RunnerStorage,
  Sharding,
  ShardingConfig,
  Snowflake,
  SqlMessageStorage
} from "@effect/cluster"
import { FileSystem } from "@effect/platform"
import { NodeFileSystem } from "@effect/platform-node"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { SqlClient } from "@effect/sql/SqlClient"
import { describe, expect, it } from "@effect/vitest"
import { Cron, Effect, Either, Layer, Ref, TestClock } from "effect"

const TestCronConfig = ShardingConfig.layer({
  entityMailboxCapacity: 10,
  entityTerminationTimeout: 0,
  entityMessagePollInterval: 1500,
  sendRetryInterval: 100
})

const queryInitialMessageIds = (dbPath: string) =>
  Effect.gen(function*() {
    const sql = yield* SqlClient
    const distinctIds =
      yield* sql`SELECT COUNT(DISTINCT message_id) as n FROM cluster_messages WHERE entity_id = ${"initial"}`
    const n = (distinctIds[0] as { n: number })?.n ?? 0
    const rowsWithInitial = yield* sql`SELECT 1 as ok FROM cluster_messages WHERE entity_id = ${"initial"} LIMIT 1`
    return { distinctMessageIds: n, rowsWithInitial }
  }).pipe(
    Effect.provide(SqliteClient.layer({ filename: dbPath })),
    Effect.scoped
  )

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

      // Share RunnerStorage across both "restart" runs so the second run has a runner assigned and can persist
      const runnerStorage = yield* RunnerStorage.makeMemory
      const shardingLayer = Sharding.layer.pipe(
        Layer.provide(Runners.layerNoop),
        Layer.provide(Layer.succeed(RunnerStorage.RunnerStorage, runnerStorage)),
        Layer.provide(RunnerHealth.layerNoop),
        Layer.provide(storageLayer),
        Layer.provide(TestCronConfig)
      )
      const fullLayer = cronLayer.pipe(Layer.provide(shardingLayer))

      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)
      const afterFirst = yield* Ref.get(runCount)
      expect(afterFirst).toBeGreaterThanOrEqual(1)

      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)
      const afterSecond = yield* Ref.get(runCount)
      expect(afterSecond).toBeGreaterThan(afterFirst)

      const { distinctMessageIds, rowsWithInitial } = yield* queryInitialMessageIds(dbPath)
      expect(distinctMessageIds).toBeGreaterThanOrEqual(2)
      expect(rowsWithInitial.length).toBeGreaterThanOrEqual(1)
    }).pipe(
      Effect.provide(NodeFileSystem.layer)
    ))

  it.scoped("InitialRun survives multiple pod restarts (3 runs, Issue 5960)", () =>
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
        name: "Issue5960ThreeRuns",
        cron,
        calculateNextRunFromPrevious: true,
        execute: Ref.update(runCount, (n) => n + 1)
      })
      const runnerStorage = yield* RunnerStorage.makeMemory
      const shardingLayer = Sharding.layer.pipe(
        Layer.provide(Runners.layerNoop),
        Layer.provide(Layer.succeed(RunnerStorage.RunnerStorage, runnerStorage)),
        Layer.provide(RunnerHealth.layerNoop),
        Layer.provide(storageLayer),
        Layer.provide(TestCronConfig)
      )
      const fullLayer = cronLayer.pipe(Layer.provide(shardingLayer))

      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)
      const afterFirst = yield* Ref.get(runCount)
      expect(afterFirst).toBeGreaterThanOrEqual(1)

      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)
      const afterSecond = yield* Ref.get(runCount)
      expect(afterSecond).toBeGreaterThan(afterFirst)

      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)
      const afterThird = yield* Ref.get(runCount)
      expect(afterThird).toBeGreaterThan(afterSecond)

      const { distinctMessageIds } = yield* queryInitialMessageIds(dbPath)
      expect(distinctMessageIds).toBeGreaterThanOrEqual(3)
    }).pipe(
      Effect.provide(NodeFileSystem.layer)
    ))

  it.scoped("runs with calculateNextRunFromPrevious false (non-initial path)", () =>
    Effect.gen(function*() {
      const runCount = yield* Ref.make(0)
      const fs = yield* FileSystem.FileSystem
      const tmpDir = yield* fs.makeTempDirectoryScoped()
      const dbPath = tmpDir + "/cluster.db"
      const storageLayer = SqlMessageStorage.layer.pipe(
        Layer.provideMerge(Snowflake.layerGenerator),
        Layer.provide(TestCronConfig),
        Layer.provide(SqliteClient.layer({ filename: dbPath }))
      )
      const cron = Cron.parse("*/1 * * * * *").pipe(Either.getOrThrow)
      const cronLayer = ClusterCron.make({
        name: "Issue5960NoInitial",
        cron,
        calculateNextRunFromPrevious: false,
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

      yield* Effect.gen(function*() {
        yield* TestClock.adjust("6 seconds")
      }).pipe(Effect.provide(fullLayer), Effect.scoped)
      const n = yield* Ref.get(runCount)
      expect(n).toBeGreaterThanOrEqual(1)
    }).pipe(
      Effect.provide(NodeFileSystem.layer)
    ))
})
