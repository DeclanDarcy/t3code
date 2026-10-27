import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { ProviderInstanceId, type OrchestrationV2ProviderThread } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Result from "effect/Result";
import * as Profile from "./CodexCognitiveProfile.ts";

it.effect("materializes the pinned base and eleven skills without copying credentials", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const state = yield* fs.makeTempDirectoryScoped();
    const profile = yield* Profile.prepareCognitiveProfile(state);
    assert.equal(profile.id, Profile.COGNITIVE_PROFILE_ID);
    assert.equal(profile.codexVersion, "0.164.0");
    assert.equal(profile.developerInstructions.match(/# Supplied cognitive skill:/g)?.length, 11);
    assert.equal(profile.config.web_search, "live");
    assert.equal(profile.config["features.shell_tool"], false);
    assert.equal(profile.config["features.code_mode_host"], false);
    const base = yield* fs.readFileString(String(profile.config.model_instructions_file));
    assert.include(base, "Cognitive Model Instructions v0.6");
    const catalog = yield* fs.readFileString(String(profile.config.model_catalog_json));
    assert.include(catalog, '"apply_patch_tool_type":null');
    assert.include(catalog, '"shell_type":"disabled"');
    assert.isFalse(yield* fs.exists(path.join(state, "auth.json")));
    yield* Profile.prepareCognitiveProfile(state);
    yield* fs.remove(String(profile.config.model_instructions_file));
    assert.isTrue(Result.isFailure(yield* Effect.result(Profile.prepareCognitiveProfile(state))));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("rejects modified runtime files rather than repairing them during resume", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const profile = yield* Profile.prepareCognitiveProfile(yield* fs.makeTempDirectoryScoped());
    yield* fs.writeFileString(String(profile.config.model_catalog_json), "{}");
    assert.isTrue(Result.isFailure(yield* Effect.result(profile.validate())));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("binds native conversations to exactly one profile and model", () =>
  Effect.gen(function* () {
    const thread = {
      nativeMetadata: { codexCognitiveProfile: Profile.COGNITIVE_PROFILE_ID },
    } as OrchestrationV2ProviderThread;
    yield* Profile.validateCognitiveBinding(true, thread);
    assert.isTrue(
      Result.isFailure(yield* Effect.result(Profile.validateCognitiveBinding(false, thread))),
    );
    assert.isTrue(
      Result.isFailure(
        yield* Effect.result(
          Profile.validateCognitiveBinding(true, {
            nativeMetadata: null,
          } as OrchestrationV2ProviderThread),
        ),
      ),
    );
    yield* Profile.validateCognitiveBinding(false, {
      nativeMetadata: null,
    } as OrchestrationV2ProviderThread);
    yield* Profile.validateCognitiveSelection({
      instanceId: ProviderInstanceId.make("cognitive"),
      model: "gpt-6.1-sol",
    });
    assert.isTrue(
      Result.isFailure(
        yield* Effect.result(
          Profile.validateCognitiveSelection({
            instanceId: ProviderInstanceId.make("cognitive"),
            model: "gpt-6-astra",
          }),
        ),
      ),
    );
  }),
);

it.effect("rejects unqualified native versions, including similar version prefixes", () =>
  Effect.gen(function* () {
    yield* Profile.validateCognitiveVersion("codex_cli_rs/0.164.0 (Windows 10.0.0)");
    for (const version of ["0.164.01", "0.165.0", "0.164.0-beta", "unknown"]) {
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(
            Profile.validateCognitiveVersion(`codex_cli_rs/${version} (Windows)`),
          ),
        ),
      );
    }
  }),
);
