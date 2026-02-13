import type * as Context from "effect/Context"
import * as Cause from "effect/Cause"
import * as Effect from "effect/Effect"

/** @internal */
export const withRun = <
  A extends {
    readonly run: (f: (...args: Array<any>) => Effect.Effect<void>) => Effect.Effect<never>
  }
>() =>
<EX, RX>(f: (write: Parameters<A["run"]>[0]) => Effect.Effect<Omit<A, "run">, EX, RX>): Effect.Effect<A, EX, RX> =>
  Effect.suspend(() => {
    let buffer: Array<[Array<any>, Context.Context<never>]> = []
    const subscriberEntries = new Map<
      (...args: Array<any>) => Effect.Effect<void>,
      Context.Context<never>
    >()
    const write = (...args: Array<any>): Effect.Effect<void> =>
      Effect.contextWith((_writeContext) => {
        if (subscriberEntries.size > 0) {
          for (const [fn, subscriberContext] of subscriberEntries.entries()) {
            try {
              Effect.runSync(
                Effect.provide(fn(...args), subscriberContext).pipe(
                  Effect.catchAllCause((cause) =>
                    Effect.logError("RpcClient Protocol: subscriber delivery failed (other subscribers still receive message)", cause).pipe(
                      Effect.asVoid
                    )
                  )
                )
              )
            } catch (defect) {
              Effect.runSync(Effect.logError("RpcClient Protocol: subscriber delivery threw", Cause.die(defect)))
            }
          }
          return Effect.void
        }
        buffer.push([args, _writeContext])
        return Effect.void
      })
    return Effect.map(f((...args) => write(...args)), (a) => ({
      ...a,
      run(fn) {
        return Effect.gen(function*() {
          const subscriberContext = yield* Effect.context<never>()
          subscriberEntries.set(fn, subscriberContext)
          for (const [args, context] of buffer) {
            yield* Effect.provide(fn(...args), context)
          }
          buffer = []

          return yield* Effect.onExit(Effect.never, () => {
            subscriberEntries.delete(fn)
            return Effect.void
          })
        })
      }
    } as A))
  })
