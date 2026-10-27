import * as Schema from "effect/Schema";
import { MessageId, RuntimeRequestId } from "./baseSchemas.ts";
import { ProjectId, ThreadId } from "./baseSchemas.ts";
import { ChatAttachment } from "./chatAttachment.ts";
import { OrchestrationMessageContext } from "./composerContext.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ExperimentHumanGuard, ExperimentOpenInput } from "./experimentHandoff.ts";
import {
  OrchestrationV2RuntimeRequestJson,
  OrchestrationV2UserInputQuestion,
  OrchestrationV2ThreadLaunchWorkspaceStrategy,
} from "./orchestrationV2.ts";
import { RuntimeMode, ProviderInteractionMode } from "./providerPolicy.ts";

export const CampaignStartInput = Schema.Struct({
  ...ExperimentOpenInput.fields,
  campaignId: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,80}$/)),
  maxAssignments: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 16 })),
});
export type CampaignStartInput = typeof CampaignStartInput.Type;
export const CampaignReadInput = Schema.Struct({
  campaignId: CampaignStartInput.fields.campaignId,
});
export const CampaignAnswer = Schema.Struct({
  campaignId: Schema.String,
  assignmentId: Schema.String,
  requestId: RuntimeRequestId,
  disposition: Schema.Literals(["answer", "insufficient", "dead_end"]),
  answers: Schema.Array(
    Schema.Struct({ questionId: Schema.String, answers: Schema.Array(Schema.String) }),
  ),
  rationale: Schema.String,
});
export const CampaignQuestion = Schema.Struct({
  obligationId: Schema.String,
  assignmentId: Schema.String,
  attemptId: Schema.String,
  requests: Schema.Array(OrchestrationV2RuntimeRequestJson),
  questions: Schema.Array(OrchestrationV2UserInputQuestion),
  messageId: MessageId,
  text: Schema.String,
  guard: ExperimentHumanGuard,
  revision: Schema.Int,
  recoveries: Schema.Int,
  responseMessageId: Schema.optional(MessageId),
  answer: Schema.optional(CampaignAnswer),
  state: Schema.Literals([
    "asking",
    "selected",
    "resolution_committed",
    "delivered",
    "human_answered",
  ]),
});
export type CampaignQuestion = typeof CampaignQuestion.Type;
export const CampaignRecord = Schema.Struct({
  ...CampaignStartInput.fields,
  assignmentIds: Schema.Array(Schema.String),
  stage: Schema.Literals([
    "planning",
    "dispatching",
    "implementing",
    "reconciling",
    "advancing",
    "terminal",
    "blocked",
  ]),
  terminal: Schema.optional(Schema.Literals(["satisfied", "dead_end", "operational_inability"])),
  reason: Schema.optional(Schema.String),
  resumeStage: Schema.optional(
    Schema.Literals(["planning", "dispatching", "implementing", "reconciling", "advancing"]),
  ),
  operationalRetries: Schema.optional(Schema.Int),
  reasoningAttempts: Schema.Int,
  implementationContinuations: Schema.Int,
  questions: Schema.Array(CampaignQuestion),
});
export type CampaignRecord = typeof CampaignRecord.Type;

export const CampaignCreateInput = Schema.Struct({
  campaignId: CampaignStartInput.fields.campaignId,
  cognitiveThreadId: ThreadId,
  projectId: ProjectId,
  text: Schema.String.check(Schema.isMaxLength(48_000)),
  attachments: Schema.Array(ChatAttachment),
  context: Schema.optional(OrchestrationMessageContext),
  implementationModel: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  workspaceStrategy: OrchestrationV2ThreadLaunchWorkspaceStrategy,
});
export type CampaignCreateInput = typeof CampaignCreateInput.Type;
export const CampaignInitialization = Schema.Struct({
  input: CampaignCreateInput,
  implementationThreadId: ThreadId,
  cognitiveModel: ModelSelection,
  profileId: Schema.String,
  prompt: Schema.String,
  title: Schema.String,
  state: Schema.Literals(["initializing", "active", "blocked"]),
  reason: Schema.optional(Schema.String),
  attempts: Schema.Int,
  attachments: Schema.Array(ChatAttachment),
  context: Schema.optional(OrchestrationMessageContext),
  attachmentEvidence: Schema.Array(
    Schema.Struct({
      attachmentId: Schema.String,
      name: Schema.String,
      sha256: Schema.String,
      text: Schema.String,
    }),
  ),
});
export type CampaignInitialization = typeof CampaignInitialization.Type;
export const CampaignSummary = Schema.Struct({
  canRetryInitialization: Schema.Boolean,
  campaignId: CampaignStartInput.fields.campaignId,
  projectId: ProjectId,
  title: Schema.String,
  cognitiveThreadId: ThreadId,
  implementationThreadId: ThreadId,
  status: Schema.Literals([
    "initializing",
    "planning",
    "implementing",
    "question",
    "reconciling",
    "satisfied",
    "dead_end",
    "blocked",
  ]),
  reason: Schema.optional(Schema.String),
  assignmentId: Schema.optional(Schema.String),
  assignmentNumber: Schema.Int,
});
export type CampaignSummary = typeof CampaignSummary.Type;
export class CampaignUiError extends Schema.TaggedError<CampaignUiError>()("CampaignUiError", {
  message: Schema.String,
}) {}
