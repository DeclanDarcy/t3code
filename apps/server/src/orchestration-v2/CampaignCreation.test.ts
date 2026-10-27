import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as FileSystem from "effect/FileSystem";
import * as SqlClient from "effect/sql/SqlClient";
import * as Result from "effect/Result";
import {
  ProjectId,
  ChatAttachmentId,
  ThreadId,
  ProviderInstanceId,
  type CampaignCreateInput,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Config from "../config.ts";
import * as Settings from "../serverSettings.ts";
import * as DocumentText from "../provider/DocumentText.ts";
import * as Creation from "./CampaignCreation.ts";
import * as Launch from "./ThreadLaunchService.ts";
import * as Threads from "./ThreadManagementService.ts";
import * as Controller from "./CampaignController.ts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";

const input: CampaignCreateInput = {
  campaignId: "ui-test",
  cognitiveThreadId: ThreadId.make("ui-test:cognitive"),
  projectId: ProjectId.make("project:test"),
  text: "  Café λ\nKeep  two spaces.\n",
  attachments: [],
  implementationModel: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-6.1-sol" },
  runtimeMode: "full-access",
  interactionMode: "default",
  workspaceStrategy: { type: "root" },
};

const harness = Effect.gen(function* () {
  const done = yield* Deferred.make<void>();
  const cognitiveReached = yield* Deferred.make<void>();
  let pauseCognitive = false;
  const fs = yield* FileSystem.FileSystem;
  const directory = yield* fs.makeTempDirectoryScoped();
  const launches: Launch.ThreadLaunchInput[] = [];
  const starts: Parameters<Controller.CampaignController["Service"]["start"]>[0][] = [];
  const dependencies = Layer.mergeAll(
    Sqlite.layerMemory,
    Config.layerTest(directory, directory),
    Settings.layerTest({
      providerInstances: {
        [ProviderInstanceId.make("codex")]: { driver: "codex" },
        [ProviderInstanceId.make("cognitive")]: {
          driver: "codex",
          config: { executionProfile: "cognitive-v1" },
        },
      },
    }),
    Layer.mock(DocumentText.DocumentText)({ ensure: () => Effect.sync(() => undefined) }),
    Layer.mock(Launch.ThreadLaunchService)({
      checkWorktreeBase: () => Effect.void,
      launch: Effect.fn(function* (request) {
        launches.push(request);
        if (pauseCognitive && request.modelSelection.instanceId === "cognitive") {
          yield* Deferred.succeed(cognitiveReached, undefined);
          return yield* Effect.never;
        }
        const threadId = request.threadId!;
        return {
          threadId,
          projection: { thread: { id: threadId } } as OrchestrationV2ThreadProjection,
          resumed: false,
        };
      }),
    }),
    Layer.mock(Threads.ThreadManagementService)({
      getThreadRecords: () => Effect.succeed({ thread: { worktreePath: null } } as never),
    }),
    Layer.mock(Controller.CampaignController)({
      start: Effect.fn(function* (request) {
        starts.push(request);
        yield* Deferred.succeed(done, undefined);
        return {} as never;
      }),
    }),
  );
  const layer = Creation.layer.pipe(
    Layer.provideMerge(dependencies),
    Layer.provideMerge(NodeServices.layer),
  );
  return {
    layer,
    launches,
    starts,
    done,
    cognitiveReached,
    setPauseCognitive: (value: boolean) => {
      pauseCognitive = value;
    },
  };
});

it.effect(
  "recovers partial member creation with the original command identities after worker reconstruction",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      yield* Effect.gen(function* () {
        const service = yield* Creation.CampaignCreation;
        h.setPauseCognitive(true);
        yield* service.create(input);
        const first = yield* Effect.forkChild(service.run);
        yield* Deferred.await(h.cognitiveReached);
        const sql = yield* SqlClient.SqlClient;
        const pending = (yield* sql<{
          state: string;
        }>`SELECT state FROM campaign_initializations`)[0]!;
        assert.equal(pending.state, "initializing");
        assert.equal(h.starts.length, 0);
        yield* Fiber.interrupt(first);
        h.setPauseCognitive(false);
        const restarted = yield* Effect.forkChild(service.run);
        yield* Deferred.await(h.done);
        yield* Fiber.interrupt(restarted);
        assert.equal(h.starts.length, 1);
        assert.equal(new Set(h.launches.map((l) => l.commandId)).size, 2);
        assert.equal(new Set(h.launches.map((l) => l.threadId)).size, 2);
        assert.equal(h.launches.length, 4);
        assert.equal(h.starts[0]!.originalInput, input.text);
      }).pipe(Effect.provide(h.layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "durably accepts exact input, replays the same identity, and launches no original implementation instruction",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      yield* Effect.gen(function* () {
        const service = yield* Creation.CampaignCreation;
        const first = yield* service.create(input);
        assert.equal(first.status, "initializing");
        assert.equal(h.launches.length, 0);
        assert.deepEqual(yield* service.create(input), first);
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(service.create({ ...input, text: input.text + "changed" })),
          ),
        );
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(service.create({ ...input, campaignId: "another-campaign" })),
          ),
        );
        const worker = yield* Effect.forkChild(service.run);
        yield* Deferred.await(h.done);
        yield* Fiber.interrupt(worker);
        assert.equal(h.launches.length, 2);
        assert.equal(h.launches[0]!.threadId, first.implementationThreadId);
        assert.equal(h.launches[0]!.modelSelection?.instanceId, "codex");
        assert.equal(h.launches[1]!.modelSelection?.instanceId, "cognitive");
        assert.isTrue(
          h.launches.every((l) => l.initialMessage === undefined && l.awaitPreparation === true),
        );
        assert.equal(h.starts[0]!.originalInput, input.text);
        assert.equal(h.starts[0]!.cognitiveThreadId, input.cognitiveThreadId);
        assert.equal(
          (yield* service.create(input)).implementationThreadId,
          first.implementationThreadId,
        );
        const sql = yield* SqlClient.SqlClient;
        const rows = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM campaign_initializations`;
        assert.equal(rows.length, 1);
        assert.equal(JSON.parse(rows[0]!.payload_json).input.text, input.text);
      }).pipe(Effect.provide(h.layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "keeps readable attachment bytes in cognitive evidence and rejects binary input without truncating it",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      yield* Effect.gen(function* () {
        const service = yield* Creation.CampaignCreation;
        const config = yield* Config.ServerConfig;
        const fs = yield* FileSystem.FileSystem;
        const body = "Exact Café λ\n\nKeep  spaces.\n";
        const file = {
          type: "file" as const,
          id: ChatAttachmentId.make(createPendingAttachmentId("md")),
          name: "evidence.md",
          mimeType: "text/markdown",
          sizeBytes: Buffer.byteLength(body),
        };
        const filePath = resolveAttachmentPath({
          attachmentsDir: config.attachmentsDir,
          attachment: file,
        })!;
        yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
        yield* fs.writeFileString(filePath, body);
        yield* service.create({ ...input, attachments: [file] });
        const sql = yield* SqlClient.SqlClient;
        const row = (yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM campaign_initializations`)[0]!;
        const record = JSON.parse(row.payload_json);
        assert.equal(record.attachmentEvidence[0].text, body);
        assert.equal(record.attachmentEvidence[0].sha256.length, 64);
        assert.notEqual(record.attachments[0].id, file.id);
        const binary = {
          ...file,
          id: ChatAttachmentId.make(createPendingAttachmentId("pdf")),
          name: "unsupported.pdf",
          mimeType: "application/pdf",
          sizeBytes: 4,
        };
        yield* fs.writeFileString(
          resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment: binary })!,
          "%PDF",
        );
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(
              service.create({
                ...input,
                campaignId: "binary",
                cognitiveThreadId: ThreadId.make("binary:cog"),
                attachments: [binary],
              }),
            ),
          ),
        );
        assert.equal((yield* service.list).length, 1);
      }).pipe(Effect.provide(h.layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect("fails closed when the configured cognitive provider is unavailable", () =>
  Effect.gen(function* () {
    const h = yield* harness;
    yield* Effect.gen(function* () {
      const settings = yield* Settings.ServerSettingsService;
      yield* settings.updateSettings({
        projectSettingsOverrides: {
          [input.projectId]: {
            providerInstances: { [ProviderInstanceId.make("cognitive")]: { enabled: false } },
          },
        },
      });
      const service = yield* Creation.CampaignCreation;
      assert.isTrue(Result.isFailure(yield* Effect.result(service.create(input))));
      assert.equal((yield* service.list).length, 0);
      assert.equal(h.launches.length, 0);
    }).pipe(Effect.provide(h.layer));
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);

it.effect(
  "rejects cognitive implementation selection and unavailable context before durable acceptance",
  () =>
    Effect.gen(function* () {
      const h = yield* harness;
      yield* Effect.gen(function* () {
        const service = yield* Creation.CampaignCreation;
        assert.isTrue(
          Result.isFailure(
            yield* Effect.result(
              service.create({
                ...input,
                implementationModel: {
                  instanceId: ProviderInstanceId.make("cognitive"),
                  model: "gpt-6.1-sol",
                },
              }),
            ),
          ),
        );
        assert.isTrue(
          Result.isFailure(yield* Effect.result(service.create({ ...input, text: " " }))),
        );
        const sql = yield* SqlClient.SqlClient;
        assert.equal((yield* sql`SELECT * FROM campaign_initializations`).length, 0);
        assert.equal(h.launches.length, 0);
      }).pipe(Effect.provide(h.layer));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
