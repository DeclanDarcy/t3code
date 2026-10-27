import {
  CampaignInitialization,
  CampaignCreateInput,
  CampaignRecord,
  CampaignUiError,
  CommandId,
  ProviderInstanceId,
  ThreadId,
  resolveProjectProviderInstanceEnabled,
  type CampaignSummary,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";
import * as FileSystem from "effect/FileSystem";
import * as Crypto from "effect/Crypto";
import * as Path from "effect/Path";
import {
  projectComposerContextForProvider,
  remapComposerContextAttachments,
} from "@t3tools/shared/composerContextReferences";
import * as Controller from "./CampaignController.ts";
import * as ThreadLaunch from "./ThreadLaunchService.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Settings from "../serverSettings.ts";
import * as Config from "../config.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import { readCognitiveAttachmentEvidence } from "../provider/CognitiveAttachmentEvidence.ts";
import { claimPendingAttachments } from "./AttachmentClaims.ts";
import {
  COGNITIVE_PROFILE_ID,
  isCognitiveConfiguration,
  prepareCognitiveProfile,
} from "../provider/CodexCognitiveProfile.ts";
import { CAMPAIGN_PROMPT } from "./CampaignPrompt.ts";

export class CampaignCreation extends Context.Service<
  CampaignCreation,
  {
    readonly create: (
      input: CampaignCreateInput,
    ) => Effect.Effect<CampaignSummary, CampaignUiError>;
    readonly retry: (id: string) => Effect.Effect<void, CampaignUiError>;
    readonly list: Effect.Effect<ReadonlyArray<CampaignSummary>, CampaignUiError>;
    readonly subscribe: Stream.Stream<ReadonlyArray<CampaignSummary>, CampaignUiError>;
    readonly run: Effect.Effect<void, never>;
  }
>()("t3/orchestration-v2/CampaignCreation") {}

const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(CampaignInitialization));
const encode = Schema.encodeSync(Schema.fromJsonString(CampaignInitialization));
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeController = Schema.decodeUnknownEffect(Schema.fromJsonString(CampaignRecord));
const isUiError = Schema.is(CampaignUiError);
const error = (message: string) => new CampaignUiError({ message });
const mapError = (cause: unknown) =>
  isUiError(cause)
    ? cause
    : error(
        cause instanceof Error
          ? cause.message
          : "Campaign initialization failed. Inspect the member conversations and server diagnostics; no unrestricted fallback was used.",
      );

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const launch = yield* ThreadLaunch.ThreadLaunchService;
  const threads = yield* Threads.ThreadManagementService;
  const controller = yield* Controller.CampaignController;
  const settings = yield* Settings.ServerSettingsService;
  const config = yield* Config.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const crypto = yield* Crypto.Crypto;
  const documents = yield* DocumentText.DocumentText;
  const pathService = yield* Path.Path;
  const wake = yield* Queue.sliding<void>(1);
  const write = (r: CampaignInitialization) =>
    sql`UPDATE campaign_initializations SET state=${r.state},payload_json=${encode(r)} WHERE campaign_id=${r.input.campaignId}`;
  const read = Effect.fn("CampaignCreation.read")(function* (id: string) {
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM campaign_initializations WHERE campaign_id=${id}`;
    if (!rows[0]) return yield* error("Campaign not found in this environment.");
    return yield* decode(rows[0].payload_json);
  });
  const summary = Effect.fn("CampaignCreation.summary")(function* (r: CampaignInitialization) {
    const rows = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM campaign_controllers WHERE campaign_id=${r.input.campaignId}`;
    const c = rows[0] ? yield* decodeController(rows[0].payload_json) : undefined;
    const status: CampaignSummary["status"] =
      r.state === "blocked"
        ? "blocked"
        : !c
          ? "initializing"
          : c.terminal === "satisfied"
            ? "satisfied"
            : c.terminal === "dead_end"
              ? "dead_end"
              : c.stage === "blocked" || c.reason?.startsWith("usage_limit:")
                ? "blocked"
                : c.stage === "implementing"
                  ? c.questions.some(
                      (q) =>
                        q.assignmentId === c.assignmentIds.at(-1) &&
                        q.state !== "delivered" &&
                        q.state !== "human_answered",
                    )
                    ? "question"
                    : "implementing"
                  : c.stage === "reconciling"
                    ? "reconciling"
                    : "planning";
    return {
      canRetryInitialization: r.state === "blocked",
      campaignId: r.input.campaignId,
      projectId: r.input.projectId,
      title: r.title,
      cognitiveThreadId: r.input.cognitiveThreadId,
      implementationThreadId: r.implementationThreadId,
      status,
      ...((r.reason ?? c?.reason) ? { reason: r.reason ?? c?.reason } : {}),
      ...(c ? { assignmentId: c.assignmentIds.at(-1) } : {}),
      assignmentNumber: c?.assignmentIds.length ?? 0,
    } satisfies CampaignSummary;
  });
  const list = sql<{
    payload_json: string;
  }>`SELECT payload_json FROM campaign_initializations ORDER BY rowid DESC`.pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (row) => decode(row.payload_json).pipe(Effect.flatMap(summary))),
    ),
    Effect.mapError(mapError),
  );
  const validate = Effect.fn("CampaignCreation.validate")(function* (
    input: CampaignCreateInput,
    cognitiveId?: ProviderInstanceId,
  ) {
    const s = yield* settings.getSettings;
    const implementation = s.providerInstances[input.implementationModel.instanceId];
    if (
      (implementation?.driver ?? input.implementationModel.instanceId) !== "codex" ||
      isCognitiveConfiguration(implementation?.config) ||
      !resolveProjectProviderInstanceEnabled(
        s,
        input.projectId,
        input.implementationModel.instanceId,
      )
    )
      return yield* error(
        "Choose an enabled ordinary Codex provider for implementation. Cognitive restrictions cannot be used for implementation.",
      );
    const candidates = Object.entries(s.providerInstances).filter(
      ([id, p]) =>
        p.driver === "codex" &&
        isCognitiveConfiguration(p.config) &&
        resolveProjectProviderInstanceEnabled(s, input.projectId, ProviderInstanceId.make(id)),
    );
    const cognitive = cognitiveId ? candidates.find(([id]) => id === cognitiveId) : candidates[0];
    if (!cognitive)
      return yield* error(
        "This environment needs an enabled Codex provider configured with executionProfile cognitive-v1. No ordinary-provider fallback is allowed.",
      );
    if (cognitive[0] === input.implementationModel.instanceId)
      return yield* error("Cognitive and implementation providers must use distinct instances.");
    yield* prepareCognitiveProfile(config.stateDir).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.provideService(Path.Path, pathService),
      Effect.mapError((e) => error(e.message)),
    );
    return { instanceId: ProviderInstanceId.make(cognitive[0]), model: "gpt-6.1-sol" };
  });
  const create = Effect.fn("CampaignCreation.create")(function* (input: CampaignCreateInput) {
    const existing = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM campaign_initializations WHERE campaign_id=${input.campaignId}`;
    if (existing[0]) {
      const r = yield* decode(existing[0].payload_json);
      if (json(r.input) !== json(input))
        return yield* error(
          "This campaign identity already accepted different input. Reopen it instead of changing a retry.",
        );
      return yield* summary(r);
    }
    if (!input.text.trim() && !input.attachments.length)
      return yield* error("Enter a campaign objective or attach supported input.");
    const cognitiveModel = yield* validate(input);
    if (input.context?.records.some((r) => r.kind === "thread"))
      return yield* error(
        "Campaign input cannot contain a local thread reference: restricted cognition cannot read that history. Include its relevant text as a supported attachment instead.",
      );
    yield* launch.checkWorktreeBase({
      projectId: input.projectId,
      workspaceStrategy: input.workspaceStrategy,
    });
    const claimed = yield* claimPendingAttachments({
      threadId: input.cognitiveThreadId,
      attachments: input.attachments,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Config.ServerConfig, config),
      Effect.provideService(DocumentText.DocumentText, documents),
    );
    const evidence = yield* readCognitiveAttachmentEvidence({
      attachments: claimed.attachments,
      attachmentsDir: config.attachmentsDir,
      resolveTextPath: documents.ensure,
    }).pipe(
      Effect.provideService(FileSystem.FileSystem, fs),
      Effect.provideService(Crypto.Crypto, crypto),
      Effect.mapError((cause) =>
        error(cause instanceof Error ? cause.message : "Cognitive attachment preparation failed."),
      ),
    );
    const context = remapComposerContextAttachments(
      input.context,
      input.attachments,
      claimed.attachments,
    );
    const projectedContext = projectComposerContextForProvider({
      text: input.text,
      records: context?.records ?? [],
    });
    if (
      Buffer.byteLength(
        json({ text: input.text, projectedContext, evidence, attachments: claimed.attachments }),
        "utf8",
      ) > 90_000
    )
      return yield* error(
        "Campaign input/context exceeds its 90 KB bounded evidence capacity. Reduce it without omitting material input.",
      );
    const r: CampaignInitialization = {
      input,
      implementationThreadId: ThreadId.make(`${input.campaignId}:implementation`),
      cognitiveModel,
      profileId: COGNITIVE_PROFILE_ID,
      prompt: CAMPAIGN_PROMPT,
      title: input.text.trim().split(/\r?\n/)[0]?.slice(0, 80) || "Campaign",
      state: "initializing",
      attempts: 0,
      attachments: claimed.attachments,
      ...(context ? { context } : {}),
      attachmentEvidence: evidence,
    };
    yield* sql`INSERT INTO campaign_initializations(campaign_id,state,payload_json)
      SELECT ${input.campaignId},'initializing',${encode(r)} WHERE NOT EXISTS (
        SELECT 1 FROM campaign_initializations WHERE campaign_id<>${input.campaignId} AND (
          json_extract(payload_json,'$.input.cognitiveThreadId') IN (${input.cognitiveThreadId},${r.implementationThreadId}) OR
          json_extract(payload_json,'$.implementationThreadId') IN (${input.cognitiveThreadId},${r.implementationThreadId})
        )) ON CONFLICT(campaign_id) DO NOTHING`;
    const accepted = yield* read(input.campaignId);
    if (json(accepted.input) !== json(input))
      return yield* error("Concurrent campaign submission used different input.");
    yield* Queue.offer(wake, undefined);
    return yield* summary(accepted);
  }, Effect.mapError(mapError));
  const initialize = Effect.fn("CampaignCreation.initialize")(function* (id: string) {
    const claimed = yield* sql<{
      payload_json: string;
    }>`UPDATE campaign_initializations SET claimed=1 WHERE campaign_id=${id} AND state='initializing' AND claimed=0 RETURNING payload_json`;
    if (!claimed[0]) return;
    const r = yield* decode(claimed[0].payload_json);
    yield* Effect.gen(function* () {
      if (r.profileId !== COGNITIVE_PROFILE_ID)
        return yield* error(
          "The accepted cognitive profile is incompatible; initialize a new campaign explicitly.",
        );
      yield* validate(r.input, r.cognitiveModel.instanceId);
      const implementation = yield* launch.launch({
        commandId: CommandId.make(`${id}:implementation:create`),
        threadId: r.implementationThreadId,
        projectId: r.input.projectId,
        title: `Implementation · ${r.title}`,
        modelSelection: r.input.implementationModel,
        runtimeMode: r.input.runtimeMode,
        interactionMode: r.input.interactionMode,
        workspaceStrategy: r.input.workspaceStrategy,
        createdBy: "user",
        creationSource: "web",
        awaitPreparation: true,
      });
      const impl = implementation.projection.thread;
      const current = yield* threads.getThreadRecords(impl.id, []);
      yield* launch.launch({
        commandId: CommandId.make(`${id}:cognitive:create`),
        threadId: r.input.cognitiveThreadId,
        projectId: r.input.projectId,
        title: `Cognitive · ${r.title}`,
        modelSelection: r.cognitiveModel,
        runtimeMode: r.input.runtimeMode,
        interactionMode: "default",
        workspaceStrategy: current.thread.worktreePath
          ? {
              type: "existing_worktree",
              worktreePath: current.thread.worktreePath,
              ...(current.thread.branch ? { branch: current.thread.branch } : {}),
            }
          : { type: "root" },
        createdBy: "user",
        creationSource: "web",
        awaitPreparation: true,
      });
      yield* controller.start({
        campaignId: id,
        assignmentId: `${id}:1`,
        cognitiveThreadId: r.input.cognitiveThreadId,
        implementationThreadId: r.implementationThreadId,
        originalInput: r.input.text,
        campaignPrompt: r.prompt,
        maxAssignments: 16,
        initialAttachments: r.attachments,
        ...(r.context ? { initialContext: r.context } : {}),
        attachmentEvidence: r.attachmentEvidence,
      });
      yield* write({ ...r, state: "active" });
    }).pipe(
      Effect.catch((cause) =>
        Effect.logError("Campaign initialization failed", { campaignId: id, cause }).pipe(
          Effect.andThen(
            write({
              ...r,
              state: r.attempts < 2 ? "initializing" : "blocked",
              attempts: r.attempts + 1,
              reason: mapError(cause).message,
            }),
          ),
        ),
      ),
      Effect.ensuring(
        sql`UPDATE campaign_initializations SET claimed=0 WHERE campaign_id=${id}`.pipe(
          Effect.orDie,
        ),
      ),
    );
  });
  const scan = sql<{
    campaign_id: string;
  }>`SELECT campaign_id FROM campaign_initializations WHERE state='initializing'`.pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(rows, (r) => initialize(r.campaign_id), { discard: true }),
    ),
  );
  const run = Effect.gen(function* () {
    yield* sql`UPDATE campaign_initializations SET claimed=0`;
    yield* Queue.offer(wake, undefined);
    yield* Queue.offer(wake, undefined).pipe(
      Effect.repeat(Schedule.spaced("5 seconds")),
      Effect.forkScoped,
    );
    return yield* Effect.forever(
      Queue.take(wake).pipe(
        Effect.andThen(scan),
        Effect.catch((cause) => Effect.logError("Campaign initialization scan failed", { cause })),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.catch((cause) => Effect.logError("Campaign initialization worker stopped", { cause })),
  );
  const subscribe = Stream.concat(
    Stream.fromEffect(list),
    Stream.tick("1 second").pipe(Stream.mapEffect(() => list)),
  ).pipe(Stream.changesWith((a, b) => json(a) === json(b)));
  return CampaignCreation.of({
    create,
    list,
    subscribe,
    run,
    retry: Effect.fn("CampaignCreation.retry")(function* (id) {
      const r = yield* read(id);
      if (r.state !== "blocked") return;
      const { reason: _reason, ...rest } = r;
      yield* write({ ...rest, state: "initializing", attempts: 0 });
      yield* Queue.offer(wake, undefined);
    }, Effect.mapError(mapError)),
  });
});
export const layer = Layer.effect(CampaignCreation, make);
