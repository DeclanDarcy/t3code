import {
  OrchestratorMcpFailure,
  type ExperimentOpenInput,
  type ExperimentRequestInput,
  type ExperimentAcceptInput,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Experiment from "../../../orchestration-v2/ExperimentHandoffService.ts";
import * as Campaign from "../../../orchestration-v2/CampaignController.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import { ExperimentToolkit } from "./tools.ts";

const mapFailure = (error: Experiment.ExperimentHandoffError) =>
  new OrchestratorMcpFailure({ code: "invalid_request", message: error.message });
const coordinator = Effect.gen(function* () {
  const scope = yield* McpInvocationContext.McpInvocationContext;
  if (!scope.client)
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "This operation requires a coordinator client, not an agent thread.",
    });
  return yield* Experiment.ExperimentHandoffService;
});

const handlers = {
  t3_campaign_start: McpToolAccess.writesEnvironment((input) =>
    coordinator.pipe(
      Effect.andThen(Campaign.CampaignController),
      Effect.flatMap((service) => service.start(input)),
      Effect.mapError(
        (error) => new OrchestratorMcpFailure({ code: "invalid_request", message: error.message }),
      ),
    ),
  ),
  t3_campaign_read: McpToolAccess.reads(({ campaignId }) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const r = yield* (yield* Campaign.CampaignController)
        .read(campaignId)
        .pipe(
          Effect.mapError(
            (error) =>
              new OrchestratorMcpFailure({ code: "invalid_request", message: error.message }),
          ),
        );
      if (
        scope.thread &&
        ![r.cognitiveThreadId, r.implementationThreadId].includes(scope.thread.threadId)
      )
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "This thread is not a campaign member.",
        });
      return r;
    }),
  ),
  t3_experiment_open: McpToolAccess.writesEnvironment((input: ExperimentOpenInput) =>
    coordinator.pipe(
      Effect.flatMap((service) => service.open(input).pipe(Effect.mapError(mapFailure))),
    ),
  ),
  t3_experiment_read: McpToolAccess.reads(({ assignmentId }) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* Experiment.ExperimentHandoffService;
      const record = yield* service.read(assignmentId).pipe(Effect.mapError(mapFailure));
      if (
        scope.thread &&
        scope.thread.threadId !== record.cognitiveThreadId &&
        scope.thread.threadId !== record.implementationThreadId
      )
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "This thread does not belong to the assignment.",
        });
      return record;
    }),
  ),
  t3_experiment_request_decision: McpToolAccess.writesEnvironment((input: ExperimentRequestInput) =>
    coordinator.pipe(
      Effect.flatMap((service) => service.requestDecision(input).pipe(Effect.mapError(mapFailure))),
    ),
  ),
  t3_experiment_accept_decision: McpToolAccess.writesEnvironment((input: ExperimentAcceptInput) =>
    coordinator.pipe(
      Effect.flatMap((service) => service.acceptDecision(input).pipe(Effect.mapError(mapFailure))),
    ),
  ),
  t3_experiment_dispatch: McpToolAccess.writesEnvironment(({ assignmentId }) =>
    coordinator.pipe(
      Effect.flatMap((service) =>
        service.dispatchAssignment(assignmentId).pipe(Effect.mapError(mapFailure)),
      ),
    ),
  ),
  t3_experiment_submit_result: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const caller = yield* McpInvocationContext.requireThreadScope(
        scope,
        "t3_experiment_submit_result",
      );
      if (caller.thread.subagent)
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Only the owning implementation thread can submit its assignment result.",
        });
      const service = yield* Experiment.ExperimentHandoffService;
      return yield* service.submitResult(input, caller.thread).pipe(
        Effect.map((record) => ({
          assignmentId: record.assignmentId,
          attemptId: record.attemptId,
          resultId: record.result!.resultId,
          accepted: true as const,
        })),
        Effect.mapError(mapFailure),
      );
    }),
  ),
} satisfies McpToolAccess.Handlers<typeof ExperimentToolkit.tools>;

export const layer = McpToolAccess.toLayer(ExperimentToolkit, handlers);
