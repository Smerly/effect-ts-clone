/**
 * Unit test for #6025: two RpcClients sharing one Protocol both receive responses.
 * Uses a fake Protocol (no HTTP/TCP) so the test passes regardless of environment.
 */
import { Rpc, RpcClient, RpcGroup } from "@effect/rpc"
import * as RpcClientModule from "@effect/rpc/RpcClient"
import type { RpcClientError } from "@effect/rpc/RpcClientError"
import type { FromClientEncoded, FromServerEncoded } from "@effect/rpc/RpcMessage"
import { assert, describe, it } from "@effect/vitest"
import { Context, Effect, Layer, Schema } from "effect"

const GetUser = Rpc.make("GetUser", {
  success: Schema.Struct({ id: Schema.String, name: Schema.String }),
  payload: Schema.Struct({ id: Schema.String })
})

const TestRpcs = RpcGroup.make(GetUser)

class FirstClient extends Context.Tag("FirstClient")<
  FirstClient,
  RpcClient.RpcClient<RpcGroup.Rpcs<typeof TestRpcs>, RpcClientError>
>() {
  static layer = Layer.scoped(FirstClient, RpcClient.make(TestRpcs))
}

class SecondClient extends Context.Tag("SecondClient")<
  SecondClient,
  RpcClient.RpcClient<RpcGroup.Rpcs<typeof TestRpcs>, RpcClientError>
>() {
  static layer = Layer.scoped(SecondClient, RpcClient.make(TestRpcs))
}

function makeFakeProtocolLayer(): Layer.Layer<RpcClientModule.Protocol> {
  return Layer.effect(
    RpcClientModule.Protocol,
    RpcClientModule.Protocol.make(
      Effect.fnUntraced(function*(writeResponse) {
        return {
          send: (request: FromClientEncoded): Effect.Effect<void> => {
            if (request._tag !== "Request") return Effect.void
            return writeResponse({
              _tag: "Exit",
              requestId: request.id,
              exit: {
                _tag: "Success",
                value: { id: "1", name: "TestUser" }
              }
            } as FromServerEncoded)
          },
          supportsAck: false,
          supportsTransferables: false
        }
      })
    )
  )
}

const layer = Layer.merge(FirstClient.layer, SecondClient.layer).pipe(
  Layer.provide(makeFakeProtocolLayer())
)

describe("RpcClient", () => {
  describe("issue #6025 - two clients sharing one Protocol", { timeout: 15_000 }, () => {
    it.effect("two clients can be acquired from one Protocol (no hang on layer build)", () =>
      Effect.scoped(
        Effect.gen(function*() {
          const first = yield* FirstClient
          const second = yield* SecondClient
          assert.ok(first)
          assert.ok(second)
        }).pipe(Effect.provide(layer))
      ))

    it.effect("second client GetUser completes when two clients share the same Protocol", () =>
      Effect.scoped(
        Effect.gen(function*() {
          yield* FirstClient
          const second = yield* SecondClient
          const user = yield* second.GetUser({ id: "1" })
          assert.deepStrictEqual(user, { id: "1", name: "TestUser" })
        }).pipe(Effect.provide(layer))
      ))
  })
})
