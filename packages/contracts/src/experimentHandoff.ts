import * as Schema from "effect/Schema";
import { MessageId, ThreadId } from "./baseSchemas.ts";
import { ProviderInstanceId } from "./providerInstance.ts";
import { ChatAttachment } from "./chatAttachment.ts";
import { OrchestrationMessageContext } from "./composerContext.ts";

const Id = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,200}$/));
const AssignmentId = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9._:-]{1,128}$/));
const Text = Schema.String.check(Schema.isMaxLength(48_000));
export const ExperimentHumanRevision = Schema.Struct({ threadId: ThreadId, sequence: Schema.Int });
export const ExperimentHumanGuard = Schema.Array(ExperimentHumanRevision).check(
  Schema.isMaxLength(2),
);
export type ExperimentHumanGuard = typeof ExperimentHumanGuard.Type;

export const ExperimentDecision = Schema.Struct({
  assignmentId: AssignmentId,
  requestId: Id,
  kind: Schema.Literals(["assign", "continue", "satisfied", "insufficient", "dead_end"]),
  markdown: Text,
  rationale: Text,
});
export type ExperimentDecision = typeof ExperimentDecision.Type;

export const ExperimentReport = Schema.Struct({ path: Text, content: Text });
export const ExperimentResult = Schema.Struct({
  assignmentId: AssignmentId,
  attemptId: Id,
  resultId: Id,
  disposition: Schema.Literals(["completed", "unable_to_complete"]),
  changes: Text,
  qualification: Schema.Array(Schema.Struct({ action: Text, result: Text })).check(
    Schema.isMaxLength(16),
  ),
  reports: Schema.Array(ExperimentReport).check(Schema.isMaxLength(16)),
  findings: Text,
});
export type ExperimentResult = typeof ExperimentResult.Type;

export const ExperimentObservation = Schema.Struct({ source: Text, content: Text });
export const ExperimentOpenInput = Schema.Struct({
  assignmentId: AssignmentId,
  cognitiveThreadId: ThreadId,
  implementationThreadId: ThreadId,
  originalInput: Text,
  campaignPrompt: Text,
  initialAttachments: Schema.optional(Schema.Array(ChatAttachment)),
  initialContext: Schema.optional(OrchestrationMessageContext),
  attachmentEvidence: Schema.optional(
    Schema.Array(
      Schema.Struct({
        attachmentId: Schema.String,
        name: Schema.String,
        sha256: Schema.String,
        text: Text,
      }),
    ),
  ),
});
export type ExperimentOpenInput = typeof ExperimentOpenInput.Type;
export const ExperimentReadInput = Schema.Struct({ assignmentId: AssignmentId });
export const ExperimentRequestInput = Schema.Struct({
  assignmentId: AssignmentId,
  phase: Schema.Literals(["assignment", "reconciliation"]),
  refresh: Schema.optional(Schema.Boolean),
  previousRequestId: Schema.optional(Id),
  observations: Schema.optional(Schema.Array(ExperimentObservation).check(Schema.isMaxLength(8))),
});
export type ExperimentRequestInput = typeof ExperimentRequestInput.Type;
export const ExperimentAcceptInput = Schema.Struct({
  assignmentId: AssignmentId,
  requestId: Id,
  messageId: MessageId,
});
export type ExperimentAcceptInput = typeof ExperimentAcceptInput.Type;

export const ExperimentDecisionJob = Schema.Struct({
  requestId: Id,
  phase: Schema.Literals(["assignment", "reconciliation"]),
  messageId: MessageId,
  text: Schema.String,
  guard: ExperimentHumanGuard,
  responseMessageId: Schema.optional(MessageId),
  decision: Schema.optional(ExperimentDecision),
  supersedesRequestId: Schema.optional(Id),
  campaignDecision: Schema.optional(Schema.Boolean),
});
export const ExperimentRecord = Schema.Struct({
  ...ExperimentOpenInput.fields,
  attemptId: Id,
  profileId: Schema.String,
  jobs: Schema.Array(ExperimentDecisionJob),
  assignment: Schema.optional(ExperimentDecision),
  result: Schema.optional(ExperimentResult),
  resultSource: Schema.optional(
    Schema.Struct({
      threadId: ThreadId,
      providerInstanceId: ProviderInstanceId,
      providerSessionId: Schema.String,
      mcpCredentialId: Schema.String,
    }),
  ),
  observations: Schema.optional(Schema.Array(ExperimentObservation)),
  reconciliation: Schema.optional(ExperimentDecision),
});
export type ExperimentRecord = typeof ExperimentRecord.Type;
