import type { ModelSelection, OrchestrationV2ProviderThread } from "@t3tools/contracts";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import profile from "./cognitive/v1.json" with { type: "json" };

export const COGNITIVE_PROFILE_ID =
  "cognitive-v1:65fcff2531da3ecaa9fa8d4ce3ee5fb87ae2e3fbd058a030090c425c76ae62a8";
export const isCognitiveConfiguration = Schema.is(
  Schema.Struct({ executionProfile: Schema.Literal("cognitive-v1") }),
);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

export class CodexCognitiveProfileError extends Schema.TaggedError<CodexCognitiveProfileError>()(
  "CodexCognitiveProfileError",
  { reason: Schema.String, cause: Schema.optional(Schema.Defect()) },
) {
  override get message() {
    return `Cognitive Codex profile unavailable: ${this.reason}`;
  }
}
const isCognitiveProfileError = Schema.is(CodexCognitiveProfileError);

export const validateCognitiveSelection = (selection: ModelSelection) =>
  selection.model === profile.model
    ? Effect.void
    : Effect.fail(new CodexCognitiveProfileError({ reason: "cognitive-v1 requires gpt-6.1-sol" }));

export const validateCognitiveVersion = (userAgent: string) =>
  userAgent.split(/[\s/();]+/).includes(profile.codexVersion)
    ? Effect.void
    : Effect.fail(
        new CodexCognitiveProfileError({ reason: `requires Codex ${profile.codexVersion}` }),
      );

export const validateCognitiveBinding = (
  cognitive: boolean,
  thread: OrchestrationV2ProviderThread,
) => {
  const binding = thread.nativeMetadata?.codexCognitiveProfile;
  return binding === (cognitive ? COGNITIVE_PROFILE_ID : undefined)
    ? Effect.void
    : Effect.fail(
        new CodexCognitiveProfileError({
          reason: "native conversation profile does not match; create a new thread",
        }),
      );
};

const disabledFeatures = [
  "shell_tool",
  "view_image",
  "apps",
  "plugins",
  "remote_plugin",
  "recommended_plugins",
  "tool_suggest",
  "multi_agent",
  "multi_agent_v2",
  "agent_message_board",
  "goals",
  "code_mode",
  "code_mode_only",
  "code_mode_host",
  "browser_use",
  "browser_use_external",
  "computer_use",
  "image_generation",
  "sleep_tool",
  "current_time_reminder",
  "token_budget",
  "context_management",
  "memories",
  "hooks",
  "workspace_dependencies",
  "skill_search",
  "skill_mcp_dependency_install",
  "realtime_conversation",
  "daemon_auto_start",
  "api_key_model_discovery",
];

export const prepareCognitiveProfile = Effect.fn("CodexCognitiveProfile.prepare")(
  function* (stateDir: string) {
    const crypto = yield* Crypto.Crypto;
    const digest = Hex.encode(
      yield* crypto.digest("SHA-256", new TextEncoder().encode(encodeJson(profile))),
    );
    if (`cognitive-v1:${digest}` !== COGNITIVE_PROFILE_ID) {
      return yield* new CodexCognitiveProfileError({ reason: "bundled profile digest mismatch" });
    }
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const directory = path.join(
      stateDir,
      "provider-profiles",
      "codex",
      COGNITIVE_PROFILE_ID.replace(":", "-"),
    );
    const files = {
      "cognitive-model-instructions.md": profile.base.content,
      "models.json": encodeJson(profile.catalog),
    };
    const exists = yield* fs.exists(directory);
    if (!exists) {
      yield* fs.makeDirectory(directory, { recursive: true });
      for (const [name, content] of Object.entries(files))
        yield* fs.writeFileString(path.join(directory, name), content);
    }
    const validate = Effect.fn("CodexCognitiveProfile.validate")(function* () {
      for (const [name, content] of Object.entries(files)) {
        const actual = yield* fs.readFileString(path.join(directory, name));
        if (actual !== content)
          return yield* new CodexCognitiveProfileError({
            reason: "materialized profile was modified",
          });
      }
    });
    yield* validate();
    const config: Record<string, Schema.Json> = {
      model: profile.model,
      model_instructions_file: path.join(directory, "cognitive-model-instructions.md"),
      model_catalog_json: path.join(directory, "models.json"),
      web_search: "live",
      include_permissions_instructions: false,
      include_environment_context: false,
      include_collaboration_mode_instructions: false,
      include_apps_instructions: false,
      project_doc_max_bytes: 0,
      project_root_markers: [],
      "skills.include_instructions": false,
      "skills.bundled.enabled": false,
      "cloud.skills.enabled": false,
      "tools.update_plan.enabled": false,
      "tools.experimental_request_user_input.enabled": false,
      "features.skip_host_skill_discovery": true,
      ...Object.fromEntries(disabledFeatures.map((name) => [`features.${name}`, false])),
    };
    return {
      id: COGNITIVE_PROFILE_ID,
      config,
      validate,
      codexVersion: profile.codexVersion,
      developerInstructions: profile.skills
        .map((skill) => `# Supplied cognitive skill: ${skill.name}\n${skill.content}`)
        .join("\n\n"),
      launchArgs:
        "--strict-config " +
        Object.entries(config)
          .map(([key, value]) => `-c ${encodeJson(`${key}=${encodeJson(value)}`)}`)
          .join(" "),
    };
  },
  Effect.mapError((cause) =>
    isCognitiveProfileError(cause)
      ? cause
      : new CodexCognitiveProfileError({ reason: "profile files could not be prepared", cause }),
  ),
);

export type CognitiveProfile = Effect.Success<ReturnType<typeof prepareCognitiveProfile>>;
