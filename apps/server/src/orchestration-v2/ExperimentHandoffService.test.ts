import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  EnvironmentId,
  ThreadId,
  type ExperimentRecord,
  type ExperimentResult,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as Schema from "effect/Schema";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as ServerSettings from "../serverSettings.ts";
import { COGNITIVE_PROFILE_ID } from "../provider/CodexCognitiveProfile.ts";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import * as CommandReceiptStore from "./CommandReceiptStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as Handoff from "./ExperimentHandoffService.ts";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import { readHumanInputRevision } from "./HumanInputRevision.ts";
import * as Campaign from "./CampaignController.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as SqlClient from "effect/sql/SqlClient";
import { NodeId, RuntimeRequestId, TurnItemId } from "@t3tools/contracts";

const cognitiveId = ThreadId.make("thread:experiment:cognitive");
const implementationId = ThreadId.make("thread:experiment:implementation");
const instanceId = ProviderInstanceId.make("codex");
const cognitiveInstanceId = ProviderInstanceId.make("cognitive");
const projectId = ProjectId.make("project:experiment");
const model = "gpt-6.1-sol";
const adapter = (id: typeof instanceId) =>
  ProviderAdapter.ProviderAdapterV2.of({
    instanceId: id,
    driver: ProviderDriverKind.make("codex"),
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: () => Effect.die("No native provider is used by these service tests."),
  });
const database = SqlitePersistence.layerMemory;
const runtime = ProviderReplayHarness.layerWithRegistry(
  { name: "experiment-handoff" },
  ProviderAdapterRegistry.layerFromAdapters([adapter(instanceId), adapter(cognitiveInstanceId)]),
  { databaseLayer: database, runEffectWorker: false },
);
const stores = Layer.merge(ProjectionStore.layer, CommandReceiptStore.layer).pipe(
  Layer.provide(database),
);
const dependencies = Layer.mergeAll(
  runtime,
  stores,
  database,
  McpProviderSessions.layer,
  ServerSettings.layerTest({
    providerInstances: {
      [instanceId]: { driver: "codex" },
      [cognitiveInstanceId]: { driver: "codex" },
    },
  }),
);
const layerThreads = ThreadManagement.layer.pipe(Layer.provideMerge(dependencies));
const layerTest = Handoff.layer.pipe(Layer.provideMerge(layerThreads));
const layerCampaign = Campaign.layer.pipe(
  Layer.provideMerge(Layer.mergeAll(layerTest, EffectOutbox.layer.pipe(Layer.provide(database)))),
);
const input = {
  assignmentId: "tc2.test",
  cognitiveThreadId: cognitiveId,
  implementationThreadId: implementationId,
  originalInput: "Caf\u00e9 \u03bb\nKeep  two spaces.\n",
  campaignPrompt: "Design one bounded experiment.",
};
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

const setup = Effect.gen(function* () {
  const orchestrator = yield* Orchestrator.OrchestratorV2;
  const sink = yield* EventSink.EventSinkV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;
  for (const [threadId, provider] of [
    [cognitiveId, cognitiveInstanceId],
    [implementationId, instanceId],
  ] as const) {
    yield* orchestrator.dispatch({
      type: "thread.create",
      commandId: CommandId.make(`create:${threadId}`),
      threadId,
      projectId,
      title: threadId,
      modelSelection: { instanceId: provider, model },
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      createdBy: "user",
      creationSource: "web",
      importedNativeThread: {
        ref: { driver: ProviderDriverKind.make("codex"), nativeId: threadId, strength: "strong" },
      },
    });
    const p = yield* projections.getThreadProjection(threadId);
    yield* sink.write({
      events: [
        {
          id: EventId.make(`native:${threadId}`),
          type: "provider-thread.updated",
          threadId,
          occurredAt: yield* DateTime.now,
          payload: {
            ...p.providerThreads[0]!,
            providerSessionId: ProviderSessionId.make(`session:${threadId}`),
            ...(threadId === cognitiveId
              ? { nativeMetadata: { codexCognitiveProfile: COGNITIVE_PROFILE_ID } }
              : {}),
          },
        },
      ],
    });
  }
  const service = yield* Handoff.ExperimentHandoffService;
  const mcp = yield* McpProviderSessions.McpProviderSessions;
  yield* mcp.set({
    environmentId: EnvironmentId.make("test"),
    threadId: implementationId,
    providerInstanceId: instanceId,
    providerSessionId: "credential-nonce",
    endpoint: "http://fixture/mcp",
    authorizationHeader: "Bearer fixture",
    browserToolsAvailable: true,
  });
  yield* service.open(input);
  return service;
});

const finishDecision = (record: ExperimentRecord, text?: string) =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const job = record.jobs.at(-1)!;
    const p = yield* projections.getThreadProjection(cognitiveId);
    const request = p.messages.find((m) => m.id === job.messageId)!;
    const run = p.runs.find((r) => r.id === request.runId)!;
    const now = yield* DateTime.now;
    const messageId = MessageId.make(`response:${job.requestId}`);
    yield* sink.write({
      events: [
        {
          id: EventId.make(`response:${job.requestId}`),
          type: "message.updated",
          threadId: cognitiveId,
          runId: run.id,
          occurredAt: now,
          payload: {
            id: messageId,
            threadId: cognitiveId,
            runId: run.id,
            nodeId: null,
            role: "assistant",
            text:
              text ??
              encodeJson({
                assignmentId: record.assignmentId,
                requestId: job.requestId,
                kind: job.phase === "assignment" ? "assign" : "satisfied",
                markdown: "Implement and test the fix.",
                rationale: "Bounded evidence.",
              }),
            attachments: [],
            streaming: false,
            createdAt: now,
            updatedAt: now,
            createdBy: "agent",
            creationSource: "provider",
          },
        },
        {
          id: EventId.make(`complete:${job.requestId}`),
          type: "run.updated",
          threadId: cognitiveId,
          runId: run.id,
          occurredAt: now,
          payload: { ...run, status: "completed", completedAt: now },
        },
      ],
    });
    return { assignmentId: record.assignmentId, requestId: job.requestId, messageId };
  });

const assigned = Effect.gen(function* () {
  const service = yield* setup;
  const requested = yield* service.requestDecision({
    assignmentId: input.assignmentId,
    phase: "assignment",
  });
  yield* service.acceptDecision(yield* finishDecision(requested.record));
  return service;
});

const result: ExperimentResult = {
  assignmentId: input.assignmentId,
  attemptId: `${input.assignmentId}:attempt:1`,
  resultId: "result.1",
  disposition: "completed",
  changes: "Fixed parser",
  qualification: [{ action: "node --test", result: "passes" }],
  reports: [{ path: "REPORT.md", content: "Actual bounded report content" }],
  findings: "No unresolved issue.",
};

const caller = {
  threadId: implementationId,
  providerInstanceId: instanceId,
  providerSessionId: "credential-nonce",
};
const progressCampaign = (id: string) =>
  Effect.gen(function* () {
    const controller = yield* Campaign.CampaignController;
    for (let step = 0; step < 8; step++) yield* controller.advance(id);
  });

it.effect(
  "preserves exact input, binds an ID once, and recovers a lost assignment acknowledgement",
  () =>
    Effect.gen(function* () {
      const service = yield* assigned;
      assert.equal((yield* service.read(input.assignmentId)).originalInput, input.originalInput);
      assert.equal((yield* service.open(input)).attemptId, result.attemptId);
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(service.open({ ...input, originalInput: "changed" })),
        ),
      );
      const first = yield* service.dispatchAssignment(input.assignmentId);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        threadId: cognitiveId,
        commandId: CommandId.make("human:after-delivery"),
        messageId: MessageId.make("human:after-delivery"),
        text: "Later human input must not cause a replay to create a second assignment.",
        attachments: [],
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "web",
      });
      const second = yield* service.dispatchAssignment(input.assignmentId);
      assert.equal(first.messageId, second.messageId);
      assert.equal(first.runId, second.runId);
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      assert.equal((yield* projections.getThreadProjection(implementationId)).runs.length, 1);
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "rejects foreign, stale and duplicate results, then delivers readable evidence once",
  () =>
    Effect.gen(function* () {
      const service = yield* assigned;
      yield* service.dispatchAssignment(input.assignmentId);
      for (const [submission, owner] of [
        [result, { ...caller, threadId: cognitiveId }],
        [result, { ...caller, providerSessionId: "obsolete-session" }],
        [{ ...result, attemptId: "obsolete" }, caller],
        [{ ...result, assignmentId: "unknown" }, caller],
      ] as const)
        assert.isTrue(
          Result.isFailure(yield* Effect.result(service.submitResult(submission, owner))),
        );
      const submitted = yield* service.submitResult(result, caller);
      assert.equal(submitted.resultSource?.mcpCredentialId, "credential-nonce");
      assert.equal(submitted.resultSource?.providerSessionId, `session:${implementationId}`);
      assert.isTrue(Result.isFailure(yield* Effect.result(service.submitResult(result, caller))));
      const requested = yield* service.requestDecision({
        assignmentId: input.assignmentId,
        phase: "reconciliation",
        observations: [{ source: "independent test process", content: "exit 0" }],
      });
      assert.include(requested.record.jobs.at(-1)!.text, result.reports[0]!.content);
      const retry = yield* service.requestDecision({
        assignmentId: input.assignmentId,
        phase: "reconciliation",
      });
      assert.equal(retry.runId, requested.runId);
      const complete = yield* service.acceptDecision(yield* finishDecision(requested.record));
      assert.equal(complete.reconciliation?.kind, "satisfied");
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(
            service.requestDecision({ assignmentId: input.assignmentId, phase: "assignment" }),
          ),
        ),
      );
    }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "rejects malformed and stale cognitive decisions without dispatching implementation",
  () =>
    Effect.gen(function* () {
      const service = yield* setup;
      const first = yield* service.requestDecision({
        assignmentId: input.assignmentId,
        phase: "assignment",
      });
      const malformed = yield* finishDecision(first.record, "not JSON");
      assert.isTrue(Result.isFailure(yield* Effect.result(service.acceptDecision(malformed))));
      const second = yield* service.requestDecision({
        assignmentId: input.assignmentId,
        phase: "assignment",
        refresh: true,
        previousRequestId: first.record.jobs.at(-1)!.requestId,
      });
      const retryRefresh = yield* service.requestDecision({
        assignmentId: input.assignmentId,
        phase: "assignment",
        refresh: true,
        previousRequestId: first.record.jobs.at(-1)!.requestId,
      });
      assert.equal(retryRefresh.runId, second.runId);
      assert.equal(retryRefresh.record.jobs.length, 2);
      const response = yield* finishDecision(second.record);
      const orchestrator = yield* Orchestrator.OrchestratorV2;
      yield* orchestrator.dispatch({
        type: "message.dispatch",
        threadId: implementationId,
        commandId: CommandId.make("human:steering"),
        messageId: MessageId.make("human:steering"),
        text: "Preserve the old API.",
        attachments: [],
        dispatchMode: { type: "defer_start" },
        createdBy: "user",
        creationSource: "web",
      });
      assert.isTrue(Result.isFailure(yield* Effect.result(service.acceptDecision(response))));
      assert.isUndefined((yield* service.read(input.assignmentId)).assignment);
      const refreshed = yield* service.requestDecision({
        assignmentId: input.assignmentId,
        phase: "assignment",
        refresh: true,
        previousRequestId: second.record.jobs.at(-1)!.requestId,
      });
      assert.include(refreshed.record.jobs.at(-1)!.text, "Preserve the old API.");
    }).pipe(Effect.provide(layerTest)),
);

it.effect("checks cross-thread human changes in the dispatch commit transaction", () =>
  Effect.gen(function* () {
    yield* setup;
    const orchestrator = yield* Orchestrator.OrchestratorV2;
    const before = yield* readHumanInputRevision(cognitiveId);
    yield* orchestrator.dispatch({
      type: "message.dispatch",
      threadId: cognitiveId,
      commandId: CommandId.make("human:race"),
      messageId: MessageId.make("human:race"),
      text: "New constraint",
      attachments: [],
      dispatchMode: { type: "defer_start" },
      createdBy: "user",
      creationSource: "web",
    });
    const command = {
      type: "message.dispatch" as const,
      threadId: implementationId,
      commandId: CommandId.make("automation:stale"),
      messageId: MessageId.make("automation:stale"),
      text: "Stale assignment",
      attachments: [],
      dispatchMode: { type: "defer_start" as const },
      createdBy: "agent" as const,
      creationSource: "server" as const,
      expectedHumanInput: [before],
    };
    assert.isTrue(Result.isFailure(yield* Effect.result(orchestrator.dispatch(command))));
    const p = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
      implementationId,
    );
    assert.isFalse(p.messages.some((m) => m.id === command.messageId));
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    assert.equal((yield* receipts.getByCommandId(command.commandId))._tag, "Some");
  }).pipe(Effect.provide(layerTest)),
);

it.effect("refuses oversized evidence without accepting a partial result", () =>
  Effect.gen(function* () {
    const service = yield* assigned;
    yield* service.dispatchAssignment(input.assignmentId);
    assert.isTrue(
      Result.isFailure(
        yield* Effect.result(
          service.submitResult(
            { ...result, reports: [{ path: "large", content: "x".repeat(96_001) }] },
            caller,
          ),
        ),
      ),
    );
    assert.isUndefined((yield* service.read(input.assignmentId)).result);
  }).pipe(Effect.provide(layerTest)),
);

it.effect.each([
  { assignmentId: "foreign", requestId: "tc2.test:assignment:1", kind: "assign" },
  { assignmentId: "tc2.test", requestId: "foreign", kind: "assign" },
  { assignmentId: "tc2.test", requestId: "tc2.test:assignment:1", kind: "satisfied" },
  {
    assignmentId: "tc2.test",
    requestId: "tc2.test:assignment:1",
    kind: "assign",
    unexpected: "not in the native schema",
  },
])("rejects a decision with invalid identity or phase: %s", (invalid) =>
  Effect.gen(function* () {
    const service = yield* setup;
    const requested = yield* service.requestDecision({
      assignmentId: input.assignmentId,
      phase: "assignment",
    });
    const response = yield* finishDecision(
      requested.record,
      encodeJson({ ...invalid, markdown: "Not authoritative.", rationale: "Fixture" }),
    );
    assert.isTrue(Result.isFailure(yield* Effect.result(service.acceptDecision(response))));
    assert.isUndefined((yield* service.read(input.assignmentId)).assignment);
  }).pipe(Effect.provide(layerTest)),
);

it.effect("retains inability-to-complete without fabricating attempted qualification", () =>
  Effect.gen(function* () {
    const service = yield* assigned;
    yield* service.dispatchAssignment(input.assignmentId);
    const unable = {
      ...result,
      disposition: "unable_to_complete" as const,
      changes: "",
      qualification: [],
      reports: [],
      findings: "Required external specification is unavailable.",
    };
    yield* service.submitResult(unable, caller);
    assert.deepEqual((yield* service.read(input.assignmentId)).result, unable);
  }).pipe(Effect.provide(layerTest)),
);

it.effect(
  "controller owns two assignments, persists continuation, and cannot resurrect a terminal campaign",
  () =>
    Effect.gen(function* () {
      const handoff = yield* setup;
      const controller = yield* Campaign.CampaignController;
      const campaign = { ...input, campaignId: "campaign.test", maxAssignments: 2 };
      yield* controller.start(campaign);
      yield* controller.advance(campaign.campaignId);
      const first = yield* handoff.read(input.assignmentId);
      yield* finishDecision(first);
      yield* Effect.all([
        progressCampaign(campaign.campaignId),
        progressCampaign(campaign.campaignId),
      ]);
      const state = yield* controller.read(campaign.campaignId);
      assert.equal(state.stage, "implementing", encodeJson(state));
      yield* handoff.submitResult(result, caller);
      yield* progressCampaign(campaign.campaignId);
      const recon = yield* handoff.read(input.assignmentId);
      yield* finishDecision(
        recon,
        encodeJson({
          assignmentId: input.assignmentId,
          requestId: recon.jobs.at(-1)!.requestId,
          kind: "continue",
          markdown: "A second meaningful behavior remains.",
          rationale: "First result is partial.",
        }),
      );
      yield* progressCampaign(campaign.campaignId);
      const secondId = `${input.assignmentId}.2`;
      const next = yield* handoff.read(secondId);
      assert.deepEqual((yield* controller.read(campaign.campaignId)).assignmentIds, [
        input.assignmentId,
        secondId,
      ]);
      yield* finishDecision(next);
      yield* progressCampaign(campaign.campaignId);
      yield* handoff.submitResult(
        {
          ...result,
          assignmentId: secondId,
          attemptId: `${secondId}:attempt:1`,
          resultId: "result.2",
        },
        caller,
      );
      yield* progressCampaign(campaign.campaignId);
      yield* finishDecision(yield* handoff.read(secondId));
      yield* progressCampaign(campaign.campaignId);
      const final = yield* controller.read(campaign.campaignId);
      assert.equal(final.terminal, "satisfied");
      yield* controller.advance(campaign.campaignId);
      assert.deepEqual(yield* controller.read(campaign.campaignId), final);
      assert.deepEqual(yield* controller.start(campaign), final);
    }).pipe(Effect.provide(layerCampaign)),
);

it.effect(
  "controller refreshes stale planning and never treats a stopped turn as assignment result",
  () =>
    Effect.gen(function* () {
      const handoff = yield* setup;
      const controller = yield* Campaign.CampaignController;
      yield* controller.start({ ...input, campaignId: "campaign.steering", maxAssignments: 2 });
      yield* controller.advance("campaign.steering");
      const first = yield* handoff.read(input.assignmentId);
      yield* finishDecision(first);
      yield* (yield* Orchestrator.OrchestratorV2).dispatch({
        type: "message.dispatch",
        commandId: CommandId.make("human:planning"),
        threadId: cognitiveId,
        messageId: MessageId.make("human:planning"),
        text: "Added requirement, not approval",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "queue_after_active" },
      });
      yield* controller.advance("campaign.steering");
      const refreshed = yield* handoff.read(input.assignmentId);
      assert.equal(
        refreshed.jobs.length,
        2,
        encodeJson(yield* controller.read("campaign.steering")),
      );
      assert.isUndefined(refreshed.assignment);
      assert.isUndefined(refreshed.result);
      assert.equal((yield* controller.read("campaign.steering")).stage, "planning");
    }).pipe(Effect.provide(layerCampaign)),
);

const campaignImplementing = Effect.gen(function* () {
  const handoff = yield* setup;
  const controller = yield* Campaign.CampaignController;
  yield* controller.start({ ...input, campaignId: "campaign.question", maxAssignments: 2 });
  yield* progressCampaign("campaign.question");
  yield* finishDecision(yield* handoff.read(input.assignmentId));
  yield* progressCampaign("campaign.question");
  return { handoff, controller };
});
const reconstructController = Campaign.CampaignController.pipe(Effect.provide(Campaign.layer));
it.effect("rechecks an already accepted terminal proposal after newer human input", () =>
  Effect.gen(function* () {
    const h = yield* setup;
    const c = yield* Campaign.CampaignController;
    yield* c.start({ ...input, campaignId: "campaign.terminal-race", maxAssignments: 1 });
    yield* c.advance("campaign.terminal-race");
    const a = yield* h.read(input.assignmentId);
    const response = yield* finishDecision(
      a,
      encodeJson({
        assignmentId: input.assignmentId,
        requestId: a.jobs[0]!.requestId,
        kind: "satisfied",
        markdown: "Evidence",
        rationale: "Satisfied then",
      }),
    );
    yield* h.acceptDecision(response);
    yield* steer("human:after-terminal-proposal");
    yield* c.advance("campaign.terminal-race");
    assert.equal((yield* c.read("campaign.terminal-race")).stage, "planning");
    assert.equal((yield* h.read(input.assignmentId)).jobs.length, 2);
  }).pipe(Effect.provide(layerCampaign)),
);
const steer = (id: string, threadId = cognitiveId) =>
  Orchestrator.OrchestratorV2.pipe(
    Effect.flatMap((o) =>
      o.dispatch({
        type: "message.dispatch",
        commandId: CommandId.make(id),
        threadId,
        messageId: MessageId.make(id),
        text: "New human steering, not approval or cancellation.",
        attachments: [],
        createdBy: "user",
        creationSource: "web",
        dispatchMode: { type: "queue_after_active" },
      }),
    ),
  );

it.effect(
  "reconciles steering after accepted decision and before dispatch without creating an implementation message",
  () =>
    Effect.gen(function* () {
      const h = yield* setup;
      const c = yield* Campaign.CampaignController;
      yield* c.start({ ...input, campaignId: "campaign.pre-dispatch", maxAssignments: 2 });
      yield* c.advance("campaign.pre-dispatch");
      yield* finishDecision(yield* h.read(input.assignmentId));
      yield* c.advance("campaign.pre-dispatch");
      assert.equal((yield* c.read("campaign.pre-dispatch")).stage, "dispatching");
      yield* steer("human:pre-dispatch");
      yield* c.advance("campaign.pre-dispatch");
      assert.equal((yield* c.read("campaign.pre-dispatch")).stage, "planning");
      const p = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
        implementationId,
      );
      assert.equal(p.messages.length, 0);
    }).pipe(Effect.provide(layerCampaign)),
);

it.effect.each(["insufficient", "satisfied", "dead_end"] as const)(
  "planning disposition %s never fabricates an implementation assignment",
  (kind) =>
    Effect.gen(function* () {
      const h = yield* setup;
      const c = yield* Campaign.CampaignController;
      yield* c.start({ ...input, campaignId: "campaign.no-assignment", maxAssignments: 2 });
      yield* c.advance("campaign.no-assignment");
      const a = yield* h.read(input.assignmentId);
      yield* finishDecision(
        a,
        encodeJson({
          assignmentId: input.assignmentId,
          requestId: a.jobs[0]!.requestId,
          kind,
          markdown: "No implementation justified yet",
          rationale: "Evidence disposition",
        }),
      );
      yield* c.advance("campaign.no-assignment");
      const r = yield* c.read("campaign.no-assignment");
      assert.equal(r.stage, kind === "insufficient" ? "planning" : "terminal");
      assert.isUndefined((yield* h.read(input.assignmentId)).assignment);
      assert.equal(
        (yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(implementationId))
          .messages.length,
        0,
      );
      if (kind !== "insufficient")
        assert.isTrue(Result.isFailure(yield* Effect.result(h.submitResult(result, caller))));
    }).pipe(Effect.provide(layerCampaign)),
);

it.effect("retains the failed cognitive run's provider reason when recovery is exhausted", () =>
  Effect.gen(function* () {
    const handoff = yield* setup;
    const controller = yield* Campaign.CampaignController;
    const campaignId = "campaign.provider-failure";
    yield* controller.start({ ...input, campaignId, maxAssignments: 2 });
    yield* controller.advance(campaignId);
    const sink = yield* EventSink.EventSinkV2;
    const projections = yield* ProjectionStore.ProjectionStoreV2;
    const detail = "Cognitive profile requires Codex 0.164.0; installed version is incompatible.";
    for (let attempt = 0; attempt < 4; attempt++) {
      const job = (yield* handoff.read(input.assignmentId)).jobs.at(-1)!;
      const p = yield* projections.getThreadProjection(cognitiveId);
      const message = p.messages.find((m) => m.id === job.messageId)!;
      const run = p.runs.find((r) => r.id === message.runId)!;
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make(`provider-failure:${attempt}`),
            type: "turn-item.updated",
            threadId: cognitiveId,
            runId: run.id,
            occurredAt: now,
            payload: {
              id: TurnItemId.make(`provider-failure:${attempt}`),
              type: "error",
              threadId: cognitiveId,
              runId: run.id,
              nodeId: null,
              providerThreadId: null,
              providerTurnId: null,
              nativeItemRef: null,
              parentItemId: null,
              ordinal: attempt,
              status: "failed",
              title: "Provider error",
              startedAt: now,
              completedAt: now,
              updatedAt: now,
              failure: { class: "validation_error", message: detail, code: null, retryable: false },
            },
          },
          {
            id: EventId.make(`failed-run:${attempt}`),
            type: "run.updated",
            threadId: cognitiveId,
            occurredAt: now,
            payload: { ...run, status: "failed", completedAt: now },
          },
        ],
      });
      yield* controller.advance(campaignId);
    }
    const blocked = yield* controller.read(campaignId);
    assert.equal(blocked.stage, "blocked");
    assert.include(blocked.reason, detail);
    assert.isUndefined((yield* handoff.read(input.assignmentId)).assignment);
  }).pipe(Effect.provide(layerCampaign)),
);

it.effect("human interruption continues the same assignment identity, never a new experiment", () =>
  Effect.gen(function* () {
    const { controller } = yield* campaignImplementing;
    const p = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
      implementationId,
    );
    const now = yield* DateTime.now;
    yield* (yield* EventSink.EventSinkV2).write({
      events: p.runs.map((run) => ({
        id: EventId.make(`human-stop:${run.id}`),
        type: "run.updated" as const,
        threadId: implementationId,
        occurredAt: now,
        payload: { ...run, status: "interrupted" as const, completedAt: now },
      })),
    });
    yield* controller.advance("campaign.question");
    yield* controller.advance("campaign.question");
    const r = yield* controller.read("campaign.question");
    assert.deepEqual(r.assignmentIds, [input.assignmentId]);
    assert.equal(r.implementationContinuations, 1);
    assert.isUndefined(
      (yield* (yield* Handoff.ExperimentHandoffService).read(input.assignmentId)).result,
    );
  }).pipe(Effect.provide(layerCampaign)),
);

it.effect(
  "reconstructs dispatch intent, lost acknowledgment, persisted result and accepted successor without new identities",
  () =>
    Effect.gen(function* () {
      const handoff = yield* setup;
      let controller = yield* reconstructController;
      const campaign = { ...input, campaignId: "campaign.recovery", maxAssignments: 2 };
      yield* controller.start(campaign);
      yield* controller.advance(campaign.campaignId);
      yield* finishDecision(yield* handoff.read(input.assignmentId));
      yield* controller.advance(campaign.campaignId);
      assert.equal((yield* controller.read(campaign.campaignId)).stage, "dispatching");
      // Simulate delivery whose acknowledgment never reached controller custody.
      const original = yield* handoff.dispatchAssignment(input.assignmentId);
      controller = yield* reconstructController;
      yield* controller.advance(campaign.campaignId);
      const p = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
        implementationId,
      );
      assert.equal(p.messages.filter((m) => m.id === original.messageId).length, 1);
      yield* handoff.submitResult(result, caller);
      controller = yield* reconstructController;
      yield* controller.advance(campaign.campaignId);
      assert.equal((yield* controller.read(campaign.campaignId)).stage, "reconciling");
      yield* controller.advance(campaign.campaignId);
      const a = yield* handoff.read(input.assignmentId);
      const job = a.jobs.at(-1)!;
      yield* finishDecision(
        a,
        encodeJson({
          assignmentId: input.assignmentId,
          requestId: job.requestId,
          kind: "continue",
          markdown: "Second behavior",
          rationale: "Partial result",
        }),
      );
      yield* controller.advance(campaign.campaignId);
      assert.equal((yield* controller.read(campaign.campaignId)).stage, "advancing");
      assert.isTrue(
        Result.isFailure(
          yield* Effect.result(handoff.open({ ...input, assignmentId: "foreign.successor" })),
        ),
      );
      controller = yield* reconstructController;
      yield* controller.advance(campaign.campaignId);
      yield* controller.advance(campaign.campaignId);
      assert.deepEqual((yield* controller.read(campaign.campaignId)).assignmentIds, [
        input.assignmentId,
        `${input.assignmentId}.2`,
      ]);
      assert.equal((yield* handoff.read(`${input.assignmentId}.2`)).jobs.length, 1);
    }).pipe(Effect.provide(layerCampaign)),
);

it.effect(
  "does not confuse lost native delivery acknowledgment with selected-answer persistence",
  () =>
    Effect.gen(function* () {
      const { controller } = yield* campaignImplementing;
      const original = yield* publishQuestion("lost-ack");
      const answer = yield* selectQuestionAnswer;
      yield* controller.advance("campaign.question");
      const sql = yield* SqlClient.SqlClient;
      const state = yield* controller.read("campaign.question");
      const q = state.questions[0]!;
      yield* sql`UPDATE orchestration_v2_effect_outbox SET status='cancelled' WHERE command_id=${`${q.obligationId}:resolve:${original.id}`}`;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const p = yield* store.getThreadProjection(implementationId);
      const now = yield* DateTime.now;
      yield* (yield* EventSink.EventSinkV2).write({
        events: p.runs.map((run) => ({
          id: EventId.make(`lost-ack-stop:${run.id}`),
          type: "run.updated" as const,
          threadId: implementationId,
          occurredAt: now,
          payload: { ...run, status: "cancelled" as const, completedAt: now },
        })),
      });
      const recovered = yield* reconstructController;
      yield* recovered.advance("campaign.question");
      const pending = yield* recovered.read("campaign.question");
      assert.deepEqual(pending.questions[0]!.answer, answer);
      assert.notEqual(pending.questions[0]!.state, "delivered");
      assert.equal(pending.questions[0]!.recoveries, 1);
      const replacement = yield* publishQuestion("lost-ack-reissued");
      yield* recovered.advance("campaign.question");
      assert.equal((yield* recovered.read("campaign.question")).questions[0]!.requests.length, 2);
      assert.equal(
        (yield* store.getThreadProjection(implementationId)).runtimeRequests.find(
          (r) => r.id === replacement.id,
        )?.status,
        "resolved",
      );
    }).pipe(Effect.provide(layerCampaign)),
);
const publishQuestion = (name: string, mode: "live" | "message" = "live") =>
  Effect.gen(function* () {
    const sink = yield* EventSink.EventSinkV2;
    const p = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
      implementationId,
    );
    const now = yield* DateTime.now;
    const requestId = RuntimeRequestId.make(`request:${name}`);
    const nodeId = NodeId.make(`node:${name}`);
    const request = {
      id: requestId,
      nodeId,
      providerTurnId: null,
      nativeRequestRef: {
        driver: ProviderDriverKind.make("codex"),
        nativeId: name,
        strength: "strong" as const,
      },
      kind: "user_input" as const,
      status: "pending" as const,
      responseCapability:
        mode === "live"
          ? { type: "live" as const, providerSessionId: p.providerThreads[0]!.providerSessionId! }
          : { type: "message" as const },
      createdAt: now,
      resolvedAt: null,
    };
    yield* sink.write({
      events: [
        {
          id: EventId.make(`session:${name}`),
          type: "provider-session.attached",
          threadId: implementationId,
          occurredAt: now,
          payload: {
            id: p.providerThreads[0]!.providerSessionId!,
            driver: ProviderDriverKind.make("codex"),
            providerInstanceId: instanceId,
            status: "waiting",
            cwd: "/fixture",
            model,
            capabilities: CodexProviderCapabilitiesV2,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
        },
        {
          id: EventId.make(`question:${name}`),
          type: "runtime-request.updated",
          threadId: implementationId,
          occurredAt: now,
          payload: request,
        },
        {
          id: EventId.make(`question-item:${name}`),
          type: "turn-item.updated",
          threadId: implementationId,
          occurredAt: now,
          payload: {
            id: TurnItemId.make(`question:${name}`),
            threadId: implementationId,
            runId: p.runs.at(-1)!.id,
            nodeId,
            providerThreadId: p.providerThreads[0]!.id,
            providerTurnId: null,
            nativeItemRef: null,
            parentItemId: null,
            ordinal: 10,
            status: "waiting",
            title: null,
            startedAt: now,
            completedAt: null,
            updatedAt: now,
            type: "user_input_request",
            requestId,
            questions: [
              {
                id: "suffix-policy",
                header: "Suffix",
                question: "Choose oversized suffix behavior",
                options: [
                  { label: "Clamp", description: "Whole resource" },
                  { label: "Reject", description: "416" },
                ],
                allowCustomAnswer: false,
              },
            ],
            ...(mode === "message" ? { responseMode: "message" as const } : {}),
          },
        },
      ],
    });
    return request;
  });
it.effect("retains multiple pending questions and discards superseded automated answer work", () =>
  Effect.gen(function* () {
    const { controller } = yield* campaignImplementing;
    const first = yield* publishQuestion("multiple-one", "message");
    const second = yield* publishQuestion("multiple-two", "message");
    yield* controller.advance("campaign.question");
    const before = yield* controller.read("campaign.question");
    assert.equal(before.questions.length, 2);
    yield* (yield* Orchestrator.OrchestratorV2).dispatch({
      type: "runtime-request.respond",
      commandId: CommandId.make("human:multiple"),
      threadId: implementationId,
      requestId: first.id,
      answers: { "suffix-policy": "Reject" },
    });
    yield* controller.advance("campaign.question");
    yield* controller.advance("campaign.question");
    const after = yield* controller.read("campaign.question");
    assert.equal(after.questions[0]!.state, "human_answered");
    assert.equal(after.questions[1]!.requests[0]!.id, second.id);
    const p = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(cognitiveId);
    assert.equal(p.messages.filter((m) => m.id === after.questions[1]!.messageId).length, 1);
  }).pipe(Effect.provide(layerCampaign)),
);

const selectQuestionAnswer = Effect.gen(function* () {
  const controller = yield* Campaign.CampaignController;
  const handoff = yield* Handoff.ExperimentHandoffService;
  yield* controller.advance("campaign.question");
  const c = yield* controller.read("campaign.question");
  const q = c.questions[0]!;
  const answer = {
    campaignId: c.campaignId,
    assignmentId: input.assignmentId,
    requestId: q.requests[0]!.id,
    disposition: "answer" as const,
    answers: [{ questionId: "suffix-policy", answers: ["Clamp"] }],
    rationale: "Explicit campaign policy.",
  };
  yield* finishDecision(
    {
      ...(yield* handoff.read(input.assignmentId)),
      jobs: [
        {
          requestId: q.obligationId,
          phase: "assignment",
          messageId: q.messageId,
          text: q.text,
          guard: q.guard,
        },
      ],
    },
    encodeJson(answer),
  );
  yield* controller.advance(c.campaignId);
  assert.equal((yield* controller.read(c.campaignId)).questions[0]!.state, "selected");
  return answer;
});

it.effect.each(["live", "message"] as const)(
  "mediates exact native %s question IDs and persists selection before resolution",
  (mode) =>
    Effect.gen(function* () {
      const { controller } = yield* campaignImplementing;
      const request = yield* publishQuestion("original", mode);
      yield* selectQuestionAnswer;
      const before = yield* controller.read("campaign.question");
      yield* controller.advance("campaign.question");
      const p = yield* (yield* ProjectionStore.ProjectionStoreV2).getThreadProjection(
        implementationId,
      );
      assert.equal(p.runtimeRequests.find((r) => r.id === request.id)?.status, "resolved");
      if (mode === "message") {
        const reply = p.messages.find((m) => m.id === `async-answer:${request.id}`)!;
        assert.equal(reply.createdBy, "agent");
        assert.equal(reply.senderThreadId, cognitiveId);
      }
      assert.deepEqual(p.runtimeRequests.find((r) => r.id === request.id)?.answers, {
        "suffix-policy": mode === "live" ? ["Clamp"] : "Clamp",
      });
      yield* controller.advance("campaign.question");
      const after = yield* controller.read("campaign.question");
      assert.deepEqual(after.questions[0]!.answer, before.questions[0]!.answer);
      const effects = yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
        CommandId.make(`${before.questions[0]!.obligationId}:resolve:${request.id}`),
      );
      assert.equal(
        effects.filter((e) => e.request.type === "runtime-request.respond").length,
        mode === "live" ? 1 : 0,
      );
      assert.isUndefined(
        (yield* (yield* Handoff.ExperimentHandoffService).read(input.assignmentId)).result,
      );
    }).pipe(Effect.provide(layerCampaign)),
);

it.effect(
  "preserves a human answer racing after cognitive selection without a second native response",
  () =>
    Effect.gen(function* () {
      const { controller } = yield* campaignImplementing;
      const request = yield* publishQuestion("human-race");
      yield* selectQuestionAnswer;
      yield* (yield* Orchestrator.OrchestratorV2).dispatch({
        type: "runtime-request.respond",
        commandId: CommandId.make("human:answer"),
        threadId: implementationId,
        requestId: request.id,
        answers: { "suffix-policy": ["Reject"] },
      });
      yield* controller.advance("campaign.question");
      const c = yield* controller.read("campaign.question");
      assert.equal(c.questions[0]!.state, "human_answered");
      assert.deepEqual(c.questions[0]!.answer?.answers[0]?.answers, ["Clamp"]);
      assert.equal(
        (yield* (yield* EffectOutbox.EffectOutboxV2).listByCommandId(
          CommandId.make(`${c.questions[0]!.obligationId}:resolve:${request.id}`),
        )).length,
        0,
      );
    }).pipe(Effect.provide(layerCampaign)),
);

it.effect(
  "recovers selected answer on a newly bound native question, never the expired handle",
  () =>
    Effect.gen(function* () {
      const { controller } = yield* campaignImplementing;
      const original = yield* publishQuestion("expired");
      yield* selectQuestionAnswer;
      const sink = yield* EventSink.EventSinkV2;
      const store = yield* ProjectionStore.ProjectionStoreV2;
      const p = yield* store.getThreadProjection(implementationId);
      const now = yield* DateTime.now;
      yield* sink.write({
        events: [
          {
            id: EventId.make("expire"),
            type: "runtime-request.updated",
            threadId: implementationId,
            occurredAt: now,
            payload: {
              ...original,
              status: "expired",
              responseCapability: { type: "not_resumable", reason: "Server restarted" },
            },
          },
          ...p.runs.map((run) => ({
            id: EventId.make(`cancel:${run.id}`),
            type: "run.updated" as const,
            threadId: implementationId,
            occurredAt: now,
            payload: { ...run, status: "cancelled" as const, completedAt: now },
          })),
        ],
      });
      yield* controller.advance("campaign.question");
      assert.equal((yield* controller.read("campaign.question")).questions[0]!.recoveries, 1);
      const replacement = yield* publishQuestion("replacement");
      yield* controller.advance("campaign.question");
      const after = yield* store.getThreadProjection(implementationId);
      assert.equal(after.runtimeRequests.find((q) => q.id === original.id)?.status, "expired");
      assert.equal(after.runtimeRequests.find((q) => q.id === replacement.id)?.status, "resolved");
      assert.equal((yield* controller.read("campaign.question")).questions[0]!.requests.length, 2);
    }).pipe(Effect.provide(layerCampaign)),
);
