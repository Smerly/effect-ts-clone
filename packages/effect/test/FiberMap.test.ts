import { assert, describe, it } from "@effect/vitest"
import { assertFalse, assertTrue, strictEqual } from "@effect/vitest/utils"
import {
  Array,
  Context,
  Cron,
  Deferred,
  Duration,
  Effect,
  Exit,
  Fiber,
  FiberMap,
  Layer,
  pipe,
  Ref,
  Schedule,
  Scope,
  TestClock,
  TestContext
} from "effect"

describe("FiberMap", () => {
  it.effect("interrupts fibers", () =>
    Effect.gen(function*() {
      const ref = yield* (Ref.make(0))
      yield* pipe(
        Effect.gen(function*() {
          const map = yield* (FiberMap.make<number>())
          yield* (
            Effect.forEach(Array.range(1, 10), (i) =>
              Effect.onInterrupt(
                Effect.never,
                () => Ref.update(ref, (n) => n + 1)
              ).pipe(
                FiberMap.run(map, i)
              ))
          )
          yield* (Effect.yieldNow())
        }),
        Effect.scoped
      )

      strictEqual(yield* (Ref.get(ref)), 10)
    }))

  it.effect("runtime", () =>
    Effect.gen(function*() {
      const ref = yield* (Ref.make(0))
      yield* pipe(
        Effect.gen(function*() {
          const map = yield* (FiberMap.make<number>())
          const run = yield* (FiberMap.runtime(map)<never>())
          Array.range(1, 10).forEach((i) =>
            run(
              i,
              Effect.onInterrupt(
                Effect.never,
                () => Ref.update(ref, (n) => n + 1)
              )
            )
          )
          yield* (Effect.yieldNow())
        }),
        Effect.scoped
      )

      strictEqual(yield* (Ref.get(ref)), 10)
    }))

  it.scoped("join", () =>
    Effect.gen(function*() {
      const map = yield* (FiberMap.make<string>())
      FiberMap.unsafeSet(map, "a", Effect.runFork(Effect.void))
      FiberMap.unsafeSet(map, "b", Effect.runFork(Effect.void))
      FiberMap.unsafeSet(map, "c", Effect.runFork(Effect.fail("fail")))
      FiberMap.unsafeSet(map, "d", Effect.runFork(Effect.fail("ignored")))
      const result = yield* pipe(FiberMap.join(map), Effect.flip)
      strictEqual(result, "fail")
    }))

  it.effect("size", () =>
    Effect.gen(function*() {
      const scope = yield* (Scope.make())
      const set = yield* pipe(FiberMap.make<string>(), Scope.extend(scope))
      FiberMap.unsafeSet(set, "a", Effect.runFork(Effect.never))
      FiberMap.unsafeSet(set, "b", Effect.runFork(Effect.never))
      strictEqual(yield* (FiberMap.size(set)), 2)
      yield* (Scope.close(scope, Exit.void))
      strictEqual(yield* (FiberMap.size(set)), 0)
    }))

  it.scoped("onlyIfMissing", () =>
    Effect.gen(function*() {
      const handle = yield* (FiberMap.make<string>())
      const fiberA = yield* (FiberMap.run(handle, "a", Effect.never))
      const fiberB = yield* (FiberMap.run(handle, "a", Effect.never, { onlyIfMissing: true }))
      const fiberC = yield* (FiberMap.run(handle, "a", Effect.never, { onlyIfMissing: true }))
      yield* (Effect.yieldNow())
      assertTrue(Exit.isInterrupted(yield* (fiberB.await)))
      assertTrue(Exit.isInterrupted(yield* (fiberC.await)))
      strictEqual(fiberA.unsafePoll(), null)
    }))

  it.scoped("runtime onlyIfMissing", () =>
    Effect.gen(function*() {
      const run = yield* (FiberMap.makeRuntime<never, string>())
      const fiberA = run("a", Effect.never)
      const fiberB = run("a", Effect.never, { onlyIfMissing: true })
      const fiberC = run("a", Effect.never, { onlyIfMissing: true })
      yield* (Effect.yieldNow())
      assertTrue(Exit.isInterrupted(yield* (fiberB.await)))
      assertTrue(Exit.isInterrupted(yield* (fiberC.await)))
      strictEqual(fiberA.unsafePoll(), null)
    }))

  it.scoped("propagateInterruption false", () =>
    Effect.gen(function*() {
      const map = yield* FiberMap.make<string>()
      const fiber = yield* FiberMap.run(map, "a", Effect.never, {
        propagateInterruption: false
      })
      yield* Effect.yieldNow()
      yield* Fiber.interrupt(fiber)
      assertFalse(yield* Deferred.isDone(map.deferred))
    }))

  it.scoped("propagateInterruption true", () =>
    Effect.gen(function*() {
      const map = yield* FiberMap.make<string>()
      const fiber = yield* FiberMap.run(map, "a", Effect.never, {
        propagateInterruption: true
      })
      yield* Effect.yieldNow()
      yield* Fiber.interrupt(fiber)
      assertTrue(Exit.isInterrupted(
        yield* FiberMap.join(map).pipe(
          Effect.exit
        )
      ))
    }))

  it.scoped("awaitEmpty", () =>
    Effect.gen(function*() {
      const map = yield* FiberMap.make<string>()
      yield* FiberMap.run(map, "a", Effect.sleep(1000))
      yield* FiberMap.run(map, "b", Effect.sleep(1000))
      yield* FiberMap.run(map, "c", Effect.sleep(1000))
      yield* FiberMap.run(map, "d", Effect.sleep(1000))

      const fiber = yield* Effect.fork(FiberMap.awaitEmpty(map))
      yield* TestClock.adjust(500)
      assert.isNull(fiber.unsafePoll())
      yield* TestClock.adjust(500)
      assert.isDefined(fiber.unsafePoll())
    }))

  it.scoped("makeRuntimePromise", () =>
    Effect.gen(function*() {
      const run = yield* FiberMap.makeRuntimePromise<never, string>()
      const result = yield* Effect.promise(() => run("a", Effect.succeed("done")))
      strictEqual(result, "done")
    }))

  /**
   * Issue 6075: FiberMap.run in Layer.scoped service can hang @effect/vitest it.scoped teardown
   * when the scheduled fiber has not reached first tick (e.g. Effect.schedule(..., Schedule.cron(...))
   * and TestClock is not advanced).
   *
   * Issue: https://github.com/Effect-TS/effect/issues/6075
   * Repro repo: https://github.com/SeanSanker/effect-fibermap-scoped-timeout-repro (see src/repro.test.ts)
   *
   * Expected: it.scoped teardown completes by interrupting/cleaning up fibers from FiberMap.run,
   * even if the cron schedule has not reached the first tick yet.
   * Bug: Teardown hangs until timeout.
   *
   * These tests must not advance TestClock before teardown in the repro case, so that the
   * "scheduled but not yet run" state is exercised. Control tests advance the clock or use
   * forkDaemon+set to verify the setup and that the hang is specific to FiberMap.run + schedule.
   *
   * If the repro test times out (teardown hangs), the bug is present. If it passes, teardown
   * completed; the bug may be fixed or not reproducible in this setup.
   *
   * Trigger: The issue repro uses Effect.provide(Svc, Live) to get the service. That path
   * uses Effect.scopedWith inside provide: it creates a new scope, builds the layer with it,
   * then closes that scope in onExit when the effect completes. Our other tests use
   * Layer.toRuntime(scope) with the test's scope, so the layer is closed via Effect.scoped
   * instead of via onExit. The hang may only occur when the FiberMap's scope is closed
   * in the onExit path (scopedWith). So we must have at least one test that uses
   * Effect.provide(Service, Live) and does not advance the clock.
   */
  describe("Issue 6075: FiberMap.run + Layer.scoped + it.scoped teardown (scheduled fiber not yet ticked)", () => {
    const REPRO_TIMEOUT_MS = 15000
    const cronEverySecond = Schedule.cron("*/1 * * * * *")
    const cronEveryMinute = Schedule.cron("0 * * * * *")

    // Exact copy from https://github.com/SeanSanker/effect-fibermap-scoped-timeout-repro src/repro.test.ts
    class Svc extends Context.Tag("@repro/Svc")<
      Svc,
      { readonly register: (name: string) => Effect.Effect<void> }
    >() {}
    const Live = Layer.scoped(
      Svc,
      Effect.gen(function*() {
        const fibers = yield* FiberMap.make<string>()
        const seen = new Set<string>()
        const register = (name: string) =>
          Effect.gen(function*() {
            if (seen.has(name)) return
            seen.add(name)
            yield* FiberMap.run(
              fibers,
              name,
              Effect.schedule(
                Effect.void,
                Schedule.cron(Cron.unsafeParse("0 * * * *"))
              ).pipe(Effect.asVoid)
            )
          })
        return Svc.of({ register })
      })
    ).pipe(Layer.extendScope)

    const ScopedService = Context.GenericTag<{
      register: (key?: string) => Effect.Effect<Fiber.RuntimeFiber<unknown, unknown>>
    }>("ScopedService")

    const serviceLayerWithFiberMapRun = Layer.scoped(
      ScopedService,
      Effect.gen(function*() {
        const map = yield* FiberMap.make<string>()
        return {
          register: (key = "key") => FiberMap.run(map, key, Effect.schedule(Effect.never, cronEverySecond))
        }
      })
    ).pipe(Layer.extendScope)

    const buildRuntime = (scope: Scope.Scope, layer = serviceLayerWithFiberMapRun) =>
      pipe(
        layer,
        Layer.provide(Layer.succeed(Scope.Scope, scope)),
        Layer.provide(TestContext.TestContext),
        Layer.toRuntime
      )

    it.scoped(
      "repro: teardown completes when FiberMap.run uses Schedule.cron and TestClock is NOT advanced (no hang)",
      () =>
        Effect.gen(function*() {
          const scope = yield* Scope.Scope
          const runtime = yield* buildRuntime(scope)
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* ScopedService
              return s
            }),
            Effect.provide(runtime)
          )
          yield* service.register()
          // Do NOT advance TestClock — scheduled fiber has not reached first tick.
          // Test body ends here; teardown runs. If bug exists, teardown hangs and test times out.
        }),
      REPRO_TIMEOUT_MS
    )

    it.scoped.each([1, 2, 3, 4, 5])(
      "repro: run %s — teardown completes (no hang) without advancing clock",
      (_runIndex) =>
        Effect.gen(function*() {
          const scope = yield* Scope.Scope
          const runtime = yield* buildRuntime(scope)
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* ScopedService
              return s
            }),
            Effect.provide(runtime)
          )
          yield* service.register()
        }),
      REPRO_TIMEOUT_MS
    )

    it.scoped(
      "repro: multiple FiberMap.run calls (3 keys), no clock advance — teardown completes",
      () =>
        Effect.gen(function*() {
          const scope = yield* Scope.Scope
          const runtime = yield* buildRuntime(scope)
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* ScopedService
              return s
            }),
            Effect.provide(runtime)
          )
          yield* service.register("a")
          yield* service.register("b")
          yield* service.register("c")
        }),
      REPRO_TIMEOUT_MS
    )

    it.scoped(
      "repro: cron every minute (first tick far away), no clock advance — teardown completes",
      () =>
        Effect.gen(function*() {
          const MinuteService = Context.GenericTag<{
            register: () => Effect.Effect<Fiber.RuntimeFiber<unknown, unknown>>
          }>("MinuteService")
          const layerMinute = Layer.scoped(
            MinuteService,
            Effect.gen(function*() {
              const map = yield* FiberMap.make<string>()
              return {
                register: () => FiberMap.run(map, "key", Effect.schedule(Effect.never, cronEveryMinute))
              }
            })
          ).pipe(Layer.extendScope)
          const scope = yield* Scope.Scope
          const runtime = yield* buildRuntime(scope, layerMinute)
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* MinuteService
              return s
            }),
            Effect.provide(runtime)
          )
          yield* service.register()
        }),
      REPRO_TIMEOUT_MS
    )

    it.scoped(
      "repro: scheduled effect did NOT run (assert no clock advance loophole)",
      () =>
        Effect.gen(function*() {
          const ranRef = yield* Ref.make(false)
          const ScopedServiceWithRef = Context.GenericTag<{
            register: () => Effect.Effect<Fiber.RuntimeFiber<unknown, unknown>>
          }>("ScopedServiceWithRef")
          const layerWithRef = Layer.scoped(
            ScopedServiceWithRef,
            Effect.gen(function*() {
              const map = yield* FiberMap.make<string>()
              return {
                register: () =>
                  FiberMap.run(
                    map,
                    "key",
                    Effect.schedule(Ref.set(ranRef, true), cronEverySecond)
                  )
              }
            })
          ).pipe(Layer.extendScope)

          const scope = yield* Scope.Scope
          const runtime = yield* pipe(
            layerWithRef,
            Layer.provide(Layer.succeed(Scope.Scope, scope)),
            Layer.provide(TestContext.TestContext),
            Layer.toRuntime
          )
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* ScopedServiceWithRef
              return s
            }),
            Effect.provide(runtime)
          )
          yield* service.register()
          // Do NOT advance TestClock.
          const didRun = yield* Ref.get(ranRef)
          assertFalse(didRun, "Scheduled effect must not have run (no clock advance)")
        }),
      REPRO_TIMEOUT_MS
    )

    // Repro repo (repro.test.ts) test 1 — passes: advance clock to first cron tick
    it.scoped(
      "repro repo: works when first tick is reached",
      () =>
        Effect.gen(function*() {
          const svc = yield* Effect.provide(Svc, Live)
          yield* svc.register("job-a")
          yield* TestClock.adjust(Duration.hours(1))
        }),
      8000
    )

    // Repro repo (repro.test.ts) test 2 — times out during teardown when no tick is reached (proves bug)
    it.scoped(
      "repro repo: times out during teardown when no tick is reached",
      () =>
        Effect.gen(function*() {
          const svc = yield* Effect.provide(Svc, Live)
          yield* svc.register("job-b")
          yield* svc.register("job-b")
        }),
      REPRO_TIMEOUT_MS
    )

    // Same Effect.provide(Svc, Live) path; multiple fibers in map — teardown must interrupt all
    it.scoped(
      "repro repo (Effect.provide): multiple keys (a,b,c) — teardown completes when no tick",
      () =>
        Effect.gen(function*() {
          const svc = yield* Effect.provide(Svc, Live)
          yield* svc.register("a")
          yield* svc.register("b")
          yield* svc.register("c")
        }),
      REPRO_TIMEOUT_MS
    )

    // Same path but Schedule.fixed (not cron) — ensures fix covers any "scheduled, not yet ticked"
    it.scoped(
      "repro repo (Effect.provide): Schedule.fixed — teardown completes when no tick",
      () =>
        Effect.gen(function*() {
          class SvcFixed extends Context.Tag("@repro/SvcFixed")<
            SvcFixed,
            { readonly register: (name: string) => Effect.Effect<void> }
          >() {}
          const LiveFixed = Layer.scoped(
            SvcFixed,
            Effect.gen(function*() {
              const fibers = yield* FiberMap.make<string>()
              const seen = new Set<string>()
              const register = (name: string) =>
                Effect.gen(function*() {
                  if (seen.has(name)) return
                  seen.add(name)
                  yield* FiberMap.run(
                    fibers,
                    name,
                    Effect.schedule(
                      Effect.void,
                      Schedule.fixed(Duration.minutes(1))
                    ).pipe(Effect.asVoid)
                  )
                })
              return SvcFixed.of({ register })
            })
          ).pipe(Layer.extendScope)

          const svc = yield* Effect.provide(SvcFixed, LiveFixed)
          yield* svc.register("key")
        }),
      REPRO_TIMEOUT_MS
    )

    // Effect.provide + assert scheduled effect did NOT run (no clock) — then teardown completes
    it.scoped(
      "repro repo (Effect.provide): scheduled effect did NOT run (assert) — teardown completes when no tick",
      () =>
        Effect.gen(function*() {
          class SvcWithRef extends Context.Tag("@repro/SvcWithRef")<
            SvcWithRef,
            {
              readonly register: (name: string) => Effect.Effect<void>
              readonly ranRef: Ref.Ref<boolean>
            }
          >() {}
          const LiveWithRef = Layer.scoped(
            SvcWithRef,
            Effect.gen(function*() {
              const fibers = yield* FiberMap.make<string>()
              const ranRef = yield* Ref.make(false)
              const seen = new Set<string>()
              const register = (name: string) =>
                Effect.gen(function*() {
                  if (seen.has(name)) return
                  seen.add(name)
                  yield* FiberMap.run(
                    fibers,
                    name,
                    Effect.schedule(
                      Ref.set(ranRef, true),
                      Schedule.cron(Cron.unsafeParse("0 * * * *"))
                    ).pipe(Effect.asVoid)
                  )
                })
              return SvcWithRef.of({ register, ranRef })
            })
          ).pipe(Layer.extendScope)

          yield* Effect.provide(
            Effect.gen(function*() {
              const svc = yield* SvcWithRef
              yield* svc.register("x")
              const didRun = yield* Ref.get(svc.ranRef)
              assertFalse(didRun, "Scheduled effect must not have run (no clock advance)")
            }),
            LiveWithRef
          )
        }),
      REPRO_TIMEOUT_MS
    )

    it.scoped(
      "control: same setup but advance TestClock to first cron tick before end — teardown completes",
      () =>
        Effect.gen(function*() {
          const scope = yield* Scope.Scope
          const runtime = yield* buildRuntime(scope)
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* ScopedService
              return s
            }),
            Effect.provide(runtime)
          )
          yield* service.register()
          yield* TestClock.adjust("2 seconds")
          // First cron tick passed; test and teardown should complete without hang.
        }),
      8000
    )

    it.scoped(
      "control: forkDaemon + FiberMap.set (no FiberMap.run) does not hang teardown when clock not advanced",
      () =>
        Effect.gen(function*() {
          const SetOnlyService = Context.GenericTag<{
            register: () => void
          }>("SetOnlyService")
          const layerSetOnly = Layer.scoped(
            SetOnlyService,
            Effect.gen(function*() {
              const map = yield* FiberMap.make<string>()
              return {
                register: () => {
                  const fiber = Effect.runFork(
                    Effect.schedule(Effect.never, cronEverySecond)
                  )
                  FiberMap.unsafeSet(map, "key", fiber)
                }
              }
            })
          ).pipe(Layer.extendScope)

          const scope = yield* Scope.Scope
          const runtime = yield* pipe(
            layerSetOnly,
            Layer.provide(Layer.succeed(Scope.Scope, scope)),
            Layer.provide(TestContext.TestContext),
            Layer.toRuntime
          )
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* SetOnlyService
              return s
            }),
            Effect.provide(runtime)
          )
          service.register()
          // Do NOT advance TestClock. Issue says this setup does NOT reproduce the timeout.
        }),
      8000
    )

    it.scoped(
      "control: FiberMap.run with Effect.never (no schedule) — teardown completes",
      () =>
        Effect.gen(function*() {
          const NoScheduleService = Context.GenericTag<{
            register: () => Effect.Effect<Fiber.RuntimeFiber<unknown, unknown>>
          }>("NoScheduleService")
          const layerNoSchedule = Layer.scoped(
            NoScheduleService,
            Effect.gen(function*() {
              const map = yield* FiberMap.make<string>()
              return {
                register: () => FiberMap.run(map, "key", Effect.never)
              }
            })
          ).pipe(Layer.extendScope)

          const scope = yield* Scope.Scope
          const runtime = yield* pipe(
            layerNoSchedule,
            Layer.provide(Layer.succeed(Scope.Scope, scope)),
            Layer.provide(TestContext.TestContext),
            Layer.toRuntime
          )
          const service = yield* pipe(
            Effect.gen(function*() {
              const s = yield* NoScheduleService
              return s
            }),
            Effect.provide(runtime)
          )
          yield* service.register()
          // No schedule; scope close should interrupt the fiber; teardown completes.
        }),
      8000
    )
  })
})
