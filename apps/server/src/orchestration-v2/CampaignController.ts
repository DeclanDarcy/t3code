import {
  CampaignAnswer,
  CampaignRecord,
  CommandId,
  MessageId,
  type CampaignStartInput,
  type CampaignQuestion,
  type ExperimentRecord,
  type ExperimentHumanGuard,
  type ThreadId,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Queue from "effect/Queue";
import * as Schedule from "effect/Schedule";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Handoff from "./ExperimentHandoffService.ts";
import * as ThreadManagement from "./ThreadManagementService.ts";
import * as EventSink from "./EventSink.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as CommandReceipts from "./CommandReceiptStore.ts";
import { humanInputIsCurrent, readHumanInputRevision } from "./HumanInputRevision.ts";

export class CampaignError extends Schema.TaggedError<CampaignError>()("CampaignError", {
  operation: Schema.String,
  detail: Schema.optional(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message() {
    return this.detail
      ? `Campaign ${this.operation}: ${this.detail}`
      : `Campaign ${this.operation} failed; inspect its durable stage and reason.`;
  }
}

export class CampaignController extends Context.Service<
  CampaignController,
  {
    readonly start: (input: CampaignStartInput) => Effect.Effect<CampaignRecord, CampaignError>;
    readonly read: (id: string) => Effect.Effect<CampaignRecord, CampaignError>;
    readonly advance: (id: string) => Effect.Effect<void, CampaignError>;
    readonly run: Effect.Effect<void, never>;
  }
>()("t3/orchestration-v2/CampaignController") {}

const encode = Schema.encodeSync(Schema.fromJsonString(CampaignRecord));
const decode = Schema.decodeUnknownEffect(Schema.fromJsonString(CampaignRecord));
const json = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeAnswer = Schema.decodeUnknownEffect(Schema.fromJsonString(CampaignAnswer), {
  onExcessProperty: "error",
});
const isCampaignError = Schema.is(CampaignError);
const terminal = (
  r: CampaignRecord,
  disposition: "satisfied" | "dead_end" | "operational_inability",
  reason: string,
): CampaignRecord => ({
  ...r,
  stage: disposition === "operational_inability" ? "blocked" : "terminal",
  terminal: disposition,
  reason,
});
const activeRun = (p: Pick<OrchestrationV2ThreadProjection, "runs">) =>
  p.runs.some((r) => ["preparing", "queued", "starting", "running", "waiting"].includes(r.status));

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const handoffs = yield* Handoff.ExperimentHandoffService;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const sink = yield* EventSink.EventSinkV2;
  const outbox = yield* EffectOutbox.EffectOutboxV2;
  const receipts = yield* CommandReceipts.CommandReceiptStoreV2;
  const wake = yield* Queue.sliding<void>(1);
  const guard = (r: CampaignRecord) =>
    Effect.forEach([r.cognitiveThreadId, r.implementationThreadId], (id) =>
      readHumanInputRevision(id).pipe(Effect.provideService(SqlClient.SqlClient, sql)),
    );
  const current = (g: ExperimentHumanGuard) =>
    humanInputIsCurrent(g).pipe(Effect.provideService(SqlClient.SqlClient, sql));
  const read = Effect.fn("Campaign.read")(
    function* (id: string) {
      const rows = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM campaign_controllers WHERE campaign_id = ${id}`;
      if (!rows[0]) return yield* new CampaignError({ operation: "unknown-campaign" });
      return yield* decode(rows[0].payload_json);
    },
    Effect.mapError((cause) => new CampaignError({ operation: "read", cause })),
  );
  const save = Effect.fn("Campaign.save")(function* (r: CampaignRecord) {
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const closed = r.stage === "terminal" || r.stage === "blocked";
        if (r.stage === "terminal") {
          const question = r.questions.findLast((q) => q.answer?.disposition === "dead_end");
          const assignment = yield* handoffs.read(r.assignmentIds.at(-1)!);
          const authority = question?.guard ?? assignment.jobs.at(-1)?.guard;
          if (!authority || !(yield* current(authority)))
            return yield* new CampaignError({ operation: "stale-terminal-decision" });
        }
        yield* sql`UPDATE campaign_controllers SET payload_json = ${encode(r)}, closed = ${closed ? 1 : 0} WHERE campaign_id = ${r.campaignId}`;
        if (closed)
          for (const id of r.assignmentIds)
            yield* sql`UPDATE experiment_handoffs SET closed=1 WHERE assignment_id=${id}`;
      }),
    );
  });
  const send = Effect.fn("Campaign.send")(function* (
    r: CampaignRecord,
    threadId: ThreadId,
    messageId: MessageId,
    text: string,
    expectedHumanInput?: ExperimentHumanGuard,
    schema?: Readonly<Record<string, Schema.Json>>,
  ) {
    const p = yield* threads.getThreadRecords(threadId, []);
    return yield* threads.sendToThread({
      projectId: p.thread.projectId,
      threadId,
      messageId,
      commandId: CommandId.make(`${messageId}:command`),
      text,
      attachments: [],
      mode: "queue",
      createdBy: "agent",
      creationSource: "server",
      senderThreadId:
        threadId === r.cognitiveThreadId ? r.implementationThreadId : r.cognitiveThreadId,
      ...(expectedHumanInput ? { expectedHumanInput } : {}),
      ...(schema ? { codexOutputSchema: schema } : {}),
    });
  });
  const response = Effect.fn("Campaign.response")(function* (
    threadId: ThreadId,
    messageId: MessageId,
  ) {
    const p = yield* threads.getThreadRecords(threadId, ["messages", "runs"]);
    const request = p.messages.find((m) => m.id === messageId);
    const run = p.runs.find((run) => run.id === request?.runId);
    if (!run || ["preparing", "queued", "starting", "running", "waiting"].includes(run.status))
      return;
    if (run.status !== "completed") {
      const failed = yield* threads.getThreadRecords(threadId, ["turnItems"], { runIds: [run.id] });
      const error = failed.turnItems.findLast((item) => item.type === "error");
      return yield* new CampaignError({
        operation: `cognitive-run-${run.status}`,
        ...(error?.type === "error" ? { detail: error.failure.message } : {}),
      });
    }
    const answers = p.messages.filter(
      (m) =>
        m.runId === run.id &&
        m.role === "assistant" &&
        m.creationSource === "provider" &&
        !m.streaming,
    );
    if (answers.length !== 1)
      return yield* new CampaignError({ operation: "ambiguous-cognitive-response" });
    return answers[0];
  });
  const questionSchema = (r: CampaignRecord, q: CampaignQuestion) => ({
    type: "object",
    properties: {
      campaignId: { type: "string", enum: [r.campaignId] },
      assignmentId: { type: "string", enum: [q.assignmentId] },
      requestId: { type: "string", enum: [q.requests[0]!.id] },
      disposition: { type: "string", enum: ["answer", "insufficient", "dead_end"] },
      answers: {
        type: "array",
        items: {
          type: "object",
          properties: {
            questionId: { type: "string", enum: q.questions.map((x) => x.id) },
            answers: { type: "array", items: { type: "string" } },
          },
          required: ["questionId", "answers"],
          additionalProperties: false,
        },
      },
      rationale: { type: "string" },
    },
    required: ["campaignId", "assignmentId", "requestId", "disposition", "answers", "rationale"],
    additionalProperties: false,
  });
  const questionText = Effect.fn("Campaign.questionText")(function* (
    r: CampaignRecord,
    q: CampaignQuestion,
    assignment: ExperimentRecord,
  ) {
    const humanInput = yield* Effect.forEach(
      [r.cognitiveThreadId, r.implementationThreadId],
      (id) =>
        threads.getThreadRecords(id, ["messages"]).pipe(
          Effect.map((p) => ({
            threadId: id,
            messages: p.messages
              .filter((m) => m.createdBy === "user")
              .map((m) => ({ id: m.id, text: m.text, attachments: m.attachments })),
          })),
        ),
    );
    const text =
      "Native implementation question; this is NOT an experiment/completion decision. Answer only from available evidence. " +
      "Preserve exact question IDs and option labels/values. Never fabricate an answer. Use insufficient for further reasoning or dead_end for a justified impasse. " +
      json({
        campaignId: r.campaignId,
        assignmentId: q.assignmentId,
        requestId: q.requests[0]!.id,
        originalInput: r.originalInput,
        campaignPrompt: r.campaignPrompt,
        assignment: assignment.assignment,
        questions: q.questions,
        previousAnswer: q.answer ?? null,
        humanInput,
      });
    if (Buffer.byteLength(text, "utf8") > 192_000)
      return yield* new CampaignError({ operation: "question-evidence-capacity" });
    return text;
  });

  const mediate = Effect.fn("Campaign.mediate")(function* (
    r: CampaignRecord,
    assignment: ExperimentRecord,
  ) {
    const p = yield* threads.getThreadRecords(r.implementationThreadId, [
      "runtimeRequests",
      "turnItems",
      "messages",
      "runs",
    ]);
    const questions = [...r.questions];
    const delivery = p.messages.find(
      (m) =>
        m.id ===
        `experiment:${assignment.assignmentId}:delivery:${assignment.assignment!.requestId}`,
    );
    const assignmentRun = p.runs.find((run) => run.id === delivery?.runId);
    for (const request of p.runtimeRequests.filter(
      (x) => x.kind === "user_input" && x.status === "pending",
    )) {
      if (questions.some((q) => q.requests.some((x) => x.id === request.id))) continue;
      const item = p.turnItems.find(
        (x) => x.type === "user_input_request" && x.requestId === request.id,
      );
      if (!item || item.type !== "user_input_request")
        return terminal(r, "operational_inability", "Persisted question has no question body.");
      const questionRun = p.runs.find((run) => run.id === item.runId);
      if (!assignmentRun || !questionRun || questionRun.ordinal < assignmentRun.ordinal) continue;
      const expired = questions.filter(
        (q) =>
          q.assignmentId === assignment.assignmentId &&
          q.state !== "delivered" &&
          q.state !== "human_answered" &&
          json(q.questions) === json(item.questions) &&
          q.requests.at(-1)!.responseCapability.type !== "message" &&
          (p.runtimeRequests.find((x) => x.id === q.requests.at(-1)!.id)?.responseCapability
            .type === "not_resumable" ||
            (q.recoveries > 0 &&
              p.runtimeRequests.find((x) => x.id === q.requests.at(-1)!.id)?.status !== "pending")),
      );
      if (expired.length > 1)
        return terminal(
          r,
          "operational_inability",
          "Ambiguous question reissue; no response sent.",
        );
      if (expired[0]) {
        const index = questions.indexOf(expired[0]);
        questions[index] = {
          ...expired[0],
          requests: [...expired[0].requests, request],
          state: expired[0].answer ? "selected" : "asking",
        };
      } else {
        if (questions.length >= 32)
          return terminal(r, "operational_inability", "Campaign reached its 32-question capacity.");
        const obligationId = `${r.campaignId}:question:${questions.length + 1}`;
        const q: CampaignQuestion = {
          obligationId,
          assignmentId: assignment.assignmentId,
          attemptId: assignment.attemptId,
          requests: [request],
          questions: item.questions,
          messageId: MessageId.make(`${obligationId}:answer:1`),
          text: "",
          guard: yield* guard(r),
          revision: 1,
          recoveries: 0,
          state: "asking",
        };
        questions.push({ ...q, text: yield* questionText(r, q, assignment) });
      }
    }
    r = { ...r, questions };
    yield* save(r);
    const index = questions.findIndex(
      (q) =>
        q.assignmentId === assignment.assignmentId &&
        q.state !== "delivered" &&
        q.state !== "human_answered",
    );
    if (index < 0) return r;
    let q = questions[index]!;
    const update = Effect.fn("Campaign.saveQuestion")(function* (next: CampaignQuestion) {
      questions[index] = next;
      r = { ...r, questions: [...questions] };
      yield* save(r);
      return r;
    });
    const request = p.runtimeRequests.find((x) => x.id === q.requests.at(-1)!.id)!;
    const commandId = CommandId.make(`${q.obligationId}:resolve:${request.id}`);
    const effects = yield* outbox.listByCommandId(commandId);
    const receipt = yield* receipts.getByCommandId(commandId);
    const ambiguousDelivery =
      request.status === "resolved" &&
      request.responseCapability.type === "live" &&
      effects.length === 0 &&
      Option.isSome(receipt);
    if (request.status === "resolved") {
      if (Option.isNone(receipt)) return yield* update({ ...q, state: "human_answered" });
      if (effects.some((e) => e.status === "failed" || e.status === "cancelled")) {
        // Admission is not native delivery: failed/ambiguous RPC effects require a fresh handle.
      } else if (
        request.responseCapability.type === "message" ||
        effects.some((e) => e.status === "succeeded")
      ) {
        return yield* update({ ...q, state: q.answer ? "delivered" : "human_answered" });
      } else if (!ambiguousDelivery) return r;
    }
    if (
      request.responseCapability.type === "not_resumable" ||
      request.status === "expired" ||
      request.status === "cancelled" ||
      ambiguousDelivery ||
      effects.some((e) => e.status === "failed" || e.status === "cancelled")
    ) {
      if (activeRun(p)) return r;
      if (q.recoveries >= 2)
        return terminal(
          r,
          "operational_inability",
          "Native question reissue capacity exhausted; original obligation and answer preserved.",
        );
      const recoveryId = MessageId.make(`${q.obligationId}:reissue:${q.recoveries + 1}`);
      const recoveryText =
        `Resume the SAME assignment ${assignment.assignmentId}, attempt ${assignment.attemptId}. Reconcile workspace/tool history before acting; do not rerun completed effects blindly. ` +
        `Native question ${request.id} lost its live response capability. This text is NOT a native answer. ` +
        `If still required, invoke the native question tool again with these EXACT questions, IDs, headers and options: ${json(q.questions)}. ` +
        `Previously selected cognitive answer (not delivered): ${json(q.answer ?? null)}. If no longer required, explicitly explain in the assignment result. Retain the same assignment/attempt.`;
      yield* send(r, r.implementationThreadId, recoveryId, recoveryText);
      return yield* update({ ...q, recoveries: q.recoveries + 1 });
    }
    if (q.state === "asking") {
      if (!(yield* current(q.guard))) {
        if (q.revision >= 3)
          return terminal(
            r,
            "operational_inability",
            "Question steering/reasoning capacity exhausted.",
          );
        q = {
          ...q,
          revision: q.revision + 1,
          messageId: MessageId.make(`${q.obligationId}:answer:${q.revision + 1}`),
          guard: yield* guard(r),
        };
        q = { ...q, text: yield* questionText(r, q, assignment) };
        yield* update(q);
      }
      yield* send(r, r.cognitiveThreadId, q.messageId, q.text, q.guard, questionSchema(r, q));
      const answerMessage = yield* response(r.cognitiveThreadId, q.messageId).pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            if (
              !isCampaignError(error) ||
              ![
                "cognitive-run-cancelled",
                "cognitive-run-interrupted",
                "cognitive-run-failed",
              ].includes(error.operation)
            )
              return yield* error;
            if (q.revision >= 3) return yield* error;
            q = {
              ...q,
              revision: q.revision + 1,
              messageId: MessageId.make(`${q.obligationId}:answer:${q.revision + 1}`),
              guard: yield* guard(r),
            };
            q = { ...q, text: yield* questionText(r, q, assignment) };
            yield* update(q);
            return undefined;
          }),
        ),
      );
      if (!answerMessage) return r;
      if (!(yield* current(q.guard))) return r;
      const answer = yield* decodeAnswer(answerMessage.text);
      if (
        answer.campaignId !== r.campaignId ||
        answer.assignmentId !== assignment.assignmentId ||
        answer.requestId !== q.requests[0]!.id
      )
        return terminal(r, "operational_inability", "Question decision identity mismatch.");
      if (answer.disposition === "dead_end") {
        const selected = yield* update({
          ...q,
          answer,
          responseMessageId: answerMessage.id,
          state: "selected",
        });
        return terminal(selected, "dead_end", answer.rationale);
      }
      if (answer.disposition === "insufficient") {
        if (q.revision >= 3)
          return terminal(
            r,
            "operational_inability",
            "Cognition could not answer the native question within the reasoning bound.",
          );
        q = {
          ...q,
          answer,
          revision: q.revision + 1,
          messageId: MessageId.make(`${q.obligationId}:answer:${q.revision + 1}`),
        };
        return yield* update({ ...q, text: yield* questionText(r, q, assignment) });
      }
      if (
        answer.answers.length !== q.questions.length ||
        new Set(answer.answers.map((a) => a.questionId)).size !== q.questions.length
      )
        return terminal(
          r,
          "operational_inability",
          "Answer must cover each original question exactly once.",
        );
      for (const question of q.questions) {
        const a = answer.answers.find((x) => x.questionId === question.id);
        if (
          !a ||
          !a.answers.length ||
          a.answers.some((x) => !x.trim()) ||
          (question.multiSelect !== true && a.answers.length !== 1) ||
          (question.allowCustomAnswer === false &&
            a.answers.some(
              (value) => !question.options.some((o) => (o.value ?? o.label) === value),
            ))
        )
          return terminal(
            r,
            "operational_inability",
            "Cognitive answer does not satisfy native question shape/options.",
          );
      }
      q = { ...q, answer, responseMessageId: answerMessage.id, state: "selected" };
      yield* update(q);
      return r;
    }
    if (q.state === "selected") {
      if (!(yield* current(q.guard)))
        return yield* update({ ...q, state: "asking", answer: undefined });
      const fresh = yield* threads.getThreadRecords(r.implementationThreadId, ["runtimeRequests"]);
      if (fresh.runtimeRequests.find((x) => x.id === request.id)?.status !== "pending") return r;
      const delivered = yield* threads
        .dispatch({
          type: "runtime-request.respond",
          commandId,
          threadId: r.implementationThreadId,
          requestId: request.id,
          expectedHumanInput: q.guard,
          createdBy: "agent",
          senderThreadId: r.cognitiveThreadId,
          answers: Object.fromEntries(
            q.answer!.answers.map((a) => [
              a.questionId,
              request.responseCapability.type === "message" ? a.answers.join("\n") : a.answers,
            ]),
          ),
        })
        .pipe(
          Effect.as(true),
          Effect.catch(() =>
            Effect.gen(function* () {
              const latest = yield* threads.getThreadRecords(r.implementationThreadId, [
                "runtimeRequests",
              ]);
              if (latest.runtimeRequests.find((x) => x.id === request.id)?.status === "resolved") {
                yield* update({ ...q, state: "human_answered" });
                return false;
              }
              if (!(yield* current(q.guard))) return false;
              return yield* new CampaignError({ operation: "native-answer-admission" });
            }),
          ),
        );
      if (!delivered) return r;
      return yield* update({ ...q, state: "resolution_committed" });
    }
    return r;
  });

  const step = Effect.fn("Campaign.step")(function* (r: CampaignRecord) {
    if (r.stage === "terminal" || r.stage === "blocked") return r;
    for (const threadId of [r.cognitiveThreadId, r.implementationThreadId]) {
      const shell = yield* threads.getThreadShell(threadId);
      if (shell?.status === "failed" && shell.lastErrorClass === "usage_limit")
        return {
          ...r,
          reason: `usage_limit:${threadId}; waiting for T3's configured usage-limit recovery, not resubmitting work.`,
        };
    }
    const id = r.assignmentIds.at(-1)!;
    if (r.stage === "advancing") {
      const previous = yield* handoffs.read(id);
      const last = previous.jobs.at(-1)!;
      if (!(yield* current(last.guard))) {
        yield* handoffs.requestDecision({
          assignmentId: id,
          phase: "reconciliation",
          refresh: true,
          previousRequestId: last.requestId,
        });
        return { ...r, stage: "reconciling" as const };
      }
      if (r.assignmentIds.length >= r.maxAssignments)
        return terminal(
          r,
          "operational_inability",
          "Campaign assignment capacity reached, not objective satisfaction.",
        );
      const nextId = `${r.assignmentId}.${r.assignmentIds.length + 1}`;
      yield* handoffs.open(
        {
          ...r,
          assignmentId: nextId,
          campaignPrompt:
            r.campaignPrompt +
            "\nPrevious accepted reconciliation (continue means propose a NEW bounded assignment):\n" +
            json(previous.reconciliation),
        },
        r.campaignId,
      );
      return {
        ...r,
        assignmentIds: [...r.assignmentIds, nextId],
        stage: "planning" as const,
        reasoningAttempts: 0,
        implementationContinuations: 0,
      };
    }
    let assignment = yield* handoffs.read(id);
    if (r.stage === "planning" || r.stage === "reconciling") {
      const phase = r.stage === "planning" ? "assignment" : "reconciliation";
      let job = assignment.jobs.at(-1);
      if (job?.phase !== phase) {
        assignment = (yield* handoffs.requestDecision({ assignmentId: id, phase })).record;
        job = assignment.jobs.at(-1)!;
      } else {
        if (!(yield* current(job.guard))) {
          if (r.reasoningAttempts >= 3)
            return terminal(
              r,
              "operational_inability",
              "Cognitive steering refresh bound reached.",
            );
          yield* handoffs.requestDecision({
            assignmentId: id,
            phase,
            refresh: true,
            previousRequestId: job.requestId,
          });
          return { ...r, reasoningAttempts: r.reasoningAttempts + 1 };
        }
        if (!job.decision) yield* handoffs.requestDecision({ assignmentId: id, phase });
      }
      const message = yield* response(r.cognitiveThreadId, job!.messageId).pipe(
        Effect.catch((error) =>
          Effect.gen(function* () {
            if (
              !isCampaignError(error) ||
              ![
                "cognitive-run-cancelled",
                "cognitive-run-interrupted",
                "cognitive-run-failed",
              ].includes(error.operation) ||
              r.reasoningAttempts >= 3
            )
              return yield* error;
            yield* handoffs.requestDecision({
              assignmentId: id,
              phase,
              refresh: true,
              previousRequestId: job!.requestId,
            });
            r = { ...r, reasoningAttempts: r.reasoningAttempts + 1 };
            yield* save(r);
            return undefined;
          }),
        ),
      );
      if (!message) return r;
      const accepted = yield* handoffs
        .acceptDecision({
          assignmentId: id,
          requestId: job!.requestId,
          messageId: message.id,
        })
        .pipe(
          Effect.catch((error) =>
            current(job!.guard).pipe(
              Effect.flatMap((fresh) => (fresh ? Effect.fail(error) : Effect.void)),
            ),
          ),
        );
      if (!accepted) return r;
      assignment = accepted;
      const decision = assignment.jobs.at(-1)!.decision!;
      if (decision.kind === "assign")
        return { ...r, stage: "dispatching" as const, reasoningAttempts: 0 };
      if (decision.kind === "satisfied" || decision.kind === "dead_end")
        return terminal(r, decision.kind, decision.rationale);
      if (decision.kind === "continue")
        return { ...r, stage: "advancing" as const, reasoningAttempts: 0 };
      if (r.reasoningAttempts >= 3)
        return terminal(
          r,
          "operational_inability",
          "Insufficient evidence after bounded cognitive reasoning; no experiment fabricated.",
        );
      yield* handoffs.requestDecision({
        assignmentId: id,
        phase,
        refresh: true,
        previousRequestId: job!.requestId,
      });
      return { ...r, reasoningAttempts: r.reasoningAttempts + 1 };
    }
    if (r.stage === "dispatching") {
      const job = assignment.jobs.find((j) => j.requestId === assignment.assignment!.requestId)!;
      if (!(yield* current(job.guard))) {
        const p = yield* threads.getThreadRecords(r.implementationThreadId, ["messages"]);
        if (!p.messages.some((m) => m.id === `experiment:${id}:delivery:${job.requestId}`)) {
          yield* handoffs.requestDecision({
            assignmentId: id,
            phase: "assignment",
            refresh: true,
            previousRequestId: job.requestId,
          });
          return { ...r, stage: "planning" as const };
        }
      }
      const sent = yield* handoffs.dispatchAssignment(id).pipe(
        Effect.as(true),
        Effect.catch((error) =>
          current(job.guard).pipe(
            Effect.flatMap((fresh) => (fresh ? Effect.fail(error) : Effect.succeed(false))),
          ),
        ),
      );
      if (!sent) return r;
      return { ...r, stage: "implementing" as const };
    }
    r = yield* mediate(r, assignment);
    if (r.terminal) return r;
    assignment = yield* handoffs.read(id);
    if (assignment.result) return { ...r, stage: "reconciling" as const, reasoningAttempts: 0 };
    const p = yield* threads.getThreadRecords(r.implementationThreadId, [
      "runs",
      "runtimeRequests",
    ]);
    if (activeRun(p) || p.runtimeRequests.some((q) => q.status === "pending")) return r;
    // A stopped turn earns no result. Continue the same assignment after inspecting its history.
    if (r.implementationContinuations >= 3)
      return terminal(
        r,
        "operational_inability",
        "Implementation ended without explicit result after bounded native continuations.",
      );
    yield* send(
      r,
      r.implementationThreadId,
      MessageId.make(`${id}:continue:${r.implementationContinuations + 1}`),
      `Continue SAME assignment ${id}, attempt ${assignment.attemptId}. Inspect existing workspace, native history and pending work before acting; do not repeat completed effects blindly. A prior turn ending did not submit a result. Complete remaining work and explicitly call t3_experiment_submit_result, or truthfully submit unable_to_complete. Preserve all human steering.`,
    );
    return { ...r, implementationContinuations: r.implementationContinuations + 1 };
  });

  const advance = Effect.fn("Campaign.advance")(
    function* (id: string) {
      const claimed = yield* sql.withTransaction(
        Effect.gen(function* () {
          const rows = yield* sql<{
            payload_json: string;
          }>`UPDATE campaign_controllers SET claimed=1 WHERE campaign_id=${id} AND claimed=0 AND closed=0 RETURNING payload_json`;
          return rows[0]?.payload_json;
        }),
      );
      if (!claimed) return;
      yield* Effect.gen(function* () {
        const r = yield* decode(claimed);
        const next = yield* step(r).pipe(
          Effect.catch((cause) =>
            Effect.logError("Campaign operation failed", {
              campaignId: id,
              stage: r.stage,
              cause,
            }).pipe(
              Effect.as({
                ...terminal(
                  r,
                  "operational_inability",
                  isCampaignError(cause)
                    ? cause.message
                    : "Controller operation failed; durable intent/receipts retained. Inspect server error cause before retrying.",
                ),
                ...(r.stage !== "terminal" && r.stage !== "blocked"
                  ? { resumeStage: r.stage }
                  : {}),
              }),
            ),
          ),
        );
        yield* save(next);
        if (json(next) !== json(r) && !next.terminal) yield* Queue.offer(wake, undefined);
      }).pipe(
        Effect.ensuring(
          sql`UPDATE campaign_controllers SET claimed=0 WHERE campaign_id=${id}`.pipe(Effect.orDie),
        ),
      );
    },
    Effect.mapError((cause) => new CampaignError({ operation: "advance", cause })),
  );
  const scan = Effect.gen(function* () {
    const rows = yield* sql<{
      campaign_id: string;
    }>`SELECT campaign_id FROM campaign_controllers WHERE closed=0`;
    yield* Effect.forEach(rows, (row) => advance(row.campaign_id), { discard: true });
  });
  const run = Effect.gen(function* () {
    // ServerOwnership excludes a second process; startup recovery runs before this worker.
    yield* sql`UPDATE campaign_controllers SET claimed=0`;
    const sequence = yield* sink.latestSequence();
    yield* sink.stream({ afterSequence: sequence }).pipe(
      Stream.filter(
        (s) =>
          s.event.type === "run.updated" ||
          s.event.type === "runtime-request.updated" ||
          (s.event.type === "message.updated" && s.event.payload.createdBy === "user"),
      ),
      Stream.runForEach(() => Queue.offer(wake, undefined)),
      Effect.forkScoped,
    );
    yield* Queue.offer(wake, undefined);
    // Result admission and outbox settlement have no domain event of their own.
    yield* Queue.offer(wake, undefined).pipe(
      Effect.repeat(Schedule.spaced("5 seconds")),
      Effect.forkScoped,
    );
    return yield* Effect.forever(
      Queue.take(wake).pipe(
        Effect.andThen(scan),
        Effect.catch((cause) => Effect.logError("Campaign scan failed", { cause })),
      ),
    );
  }).pipe(
    Effect.scoped,
    Effect.catch((cause) => Effect.logError("Campaign worker stopped", { cause })),
  );
  return CampaignController.of({
    read,
    advance,
    run,
    start: Effect.fn("Campaign.start")(
      function* (input) {
        const existing = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM campaign_controllers WHERE campaign_id=${input.campaignId}`;
        if (existing[0]) {
          let r = yield* decode(existing[0].payload_json);
          for (const key of [
            "assignmentId",
            "originalInput",
            "campaignPrompt",
            "cognitiveThreadId",
            "implementationThreadId",
            "maxAssignments",
          ] as const)
            if (r[key] !== input[key])
              return yield* new CampaignError({ operation: "identity-conflict" });
          if (r.stage === "blocked" && r.resumeStage && (r.operationalRetries ?? 0) < 3) {
            const { terminal: _terminal, reason: _reason, resumeStage, ...rest } = r;
            r = {
              ...rest,
              stage: resumeStage,
              operationalRetries: (r.operationalRetries ?? 0) + 1,
            };
            yield* save(r);
          }
          yield* Queue.offer(wake, undefined);
          return r;
        }
        const r: CampaignRecord = {
          ...input,
          assignmentIds: [input.assignmentId],
          stage: "planning",
          reasoningAttempts: 0,
          implementationContinuations: 0,
          questions: [],
        };
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* handoffs.open(input, input.campaignId);
            yield* sql`INSERT INTO campaign_controllers(campaign_id,cognitive_thread_id,implementation_thread_id,payload_json)
            VALUES(${r.campaignId},${r.cognitiveThreadId},${r.implementationThreadId},${encode(r)})`;
          }),
        );
        yield* Queue.offer(wake, undefined);
        return r;
      },
      Effect.mapError((cause) => new CampaignError({ operation: "start", cause })),
    ),
  });
});
export const layer = Layer.effect(CampaignController, make);
