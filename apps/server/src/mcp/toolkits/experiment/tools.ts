import {
  ExperimentAcceptInput,
  CampaignStartInput,
  CampaignReadInput,
  CampaignRecord,
  ExperimentOpenInput,
  ExperimentReadInput,
  ExperimentRecord,
  ExperimentRequestInput,
  ExperimentResult,
  MessageId,
  OrchestratorMcpFailure,
  RunId,
} from "@t3tools/contracts";
import { Tool, Toolkit } from "effect/ai";
import * as Schema from "effect/Schema";
import * as Experiment from "../../../orchestration-v2/ExperimentHandoffService.ts";
import * as Campaign from "../../../orchestration-v2/CampaignController.ts";
import * as ThreadManagement from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const common = {
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    Experiment.ExperimentHandoffService,
    ThreadManagement.ThreadManagementService,
    McpInvocationContext.McpInvocationContext,
  ],
};
const Delivery = Schema.Struct({ record: ExperimentRecord, messageId: MessageId, runId: RunId });

export const ExperimentToolkit = Toolkit.make(
  Tool.make("t3_campaign_start", {
    ...common,
    dependencies: [...common.dependencies, Campaign.CampaignController],
    parameters: CampaignStartInput,
    success: CampaignRecord,
    description:
      "Coordinator client: start/recover a durable autonomous campaign on two existing threads. The server owns further progression and native question mediation. Retrying identical input recovers the campaign.",
  }).annotate(Tool.Readonly, false),
  Tool.make("t3_campaign_read", {
    ...common,
    dependencies: [...common.dependencies, Campaign.CampaignController],
    parameters: CampaignReadInput,
    success: CampaignRecord,
    description:
      "Read durable campaign stage, assignment identities, question obligations, selected answers and terminal/operational disposition.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_experiment_open", {
    ...common,
    parameters: ExperimentOpenInput,
    success: ExperimentRecord,
    description:
      "Coordinator client: bind one bounded engineering assignment to two existing cognitive/implementation threads. Same ID and input recover the existing record.",
  }).annotate(Tool.Readonly, false),
  Tool.make("t3_experiment_read", {
    ...common,
    parameters: ExperimentReadInput,
    success: ExperimentRecord,
    description:
      "Read the durable assignment, decision requests, explicit result, and reconciliation. A finished provider turn alone is not an assignment result.",
  }).annotate(Tool.Readonly, true),
  Tool.make("t3_experiment_request_decision", {
    ...common,
    parameters: ExperimentRequestInput,
    success: Delivery,
    description:
      "Coordinator client: queue a correlated native-schema cognitive assignment or reconciliation request. Retries reuse the message. Refresh only a stale decision; report content is included, not fetched by cognition.",
  }).annotate(Tool.Readonly, false),
  Tool.make("t3_experiment_accept_decision", {
    ...common,
    parameters: ExperimentAcceptInput,
    success: ExperimentRecord,
    description:
      "Coordinator client: validate a specific persisted cognitive response against request identity, phase, and current human input. Malformed or stale responses do not advance the assignment.",
  }).annotate(Tool.Readonly, false),
  Tool.make("t3_experiment_dispatch", {
    ...common,
    parameters: ExperimentReadInput,
    success: Delivery,
    description:
      "Coordinator client: queue the accepted assignment to its bound implementation thread. Retries recover the same durable command/message, never a new assignment.",
  }).annotate(Tool.Readonly, false),
  Tool.make("t3_experiment_submit_result", {
    ...common,
    parameters: ExperimentResult,
    success: Schema.Struct({
      assignmentId: Schema.String,
      attemptId: Schema.String,
      resultId: Schema.String,
      accepted: Schema.Literal(true),
    }),
    description:
      "Implementation thread: explicitly submit completion or inability for your delivered assignment/attempt. Include changes, actual qualification actions/results, report paths AND readable bodies, and unresolved findings. Duplicate, wrong-thread, stale or oversized submissions fail; read the existing record after a lost response.",
  }).annotate(Tool.Readonly, false),
);
