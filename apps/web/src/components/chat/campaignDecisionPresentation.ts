import {
  CampaignAnswer,
  ExperimentDecision,
  ExperimentResult,
  OrchestrationV2UserInputQuestion,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import * as Result from "effect/Result";

const decode = Schema.decodeUnknownResult(
  Schema.fromJsonString(Schema.Union([ExperimentDecision, CampaignAnswer])),
);
export function campaignDecisionPresentation(text: string): string {
  const parsed = decode(text);
  if (Result.isFailure(parsed)) return text;
  const value = parsed.success;
  if ("markdown" in value)
    return `### ${value.kind.replaceAll("_", " ")}\n\n${value.markdown}\n\n${value.rationale}`;
  return `### Question response · ${value.disposition}\n\n${value.rationale}\n\n${value.answers.map((a) => `- ${a.questionId}: ${a.answers.join(", ")}`).join("\n")}`;
}

const evidence = Schema.decodeUnknownResult(
  Schema.fromJsonString(
    Schema.Struct({
      originalInput: Schema.String,
      assignmentId: Schema.String,
      phase: Schema.String,
      implementationReport: Schema.NullOr(
        Schema.Struct({ source: Schema.String, value: ExperimentResult }),
      ),
      attachmentEvidence: Schema.optional(
        Schema.Array(Schema.Struct({ name: Schema.String, text: Schema.String })),
      ),
    }),
  ),
);
const question = Schema.decodeUnknownResult(
  Schema.fromJsonString(
    Schema.Struct({
      assignmentId: Schema.String,
      questions: Schema.Array(OrchestrationV2UserInputQuestion),
    }),
  ),
);

export function campaignRequestPresentation(text: string): string {
  if (text.startsWith("Experiment decision request.")) {
    const parsed = evidence(text.slice(text.indexOf("\n") + 1));
    if (Result.isFailure(parsed)) return text;
    const value = parsed.success;
    const result = value.implementationReport?.value;
    return `### ${value.phase === "assignment" ? "Planning request" : "Qualification reconciliation"}\n\n${value.originalInput}\n\n${value.attachmentEvidence?.map((a) => `### Input: ${a.name}\n\n${a.text}`).join("\n\n") ?? ""}${result ? `\n\n### Implementation result · ${value.assignmentId}\n\n${result.changes}\n\n${result.qualification.map((q) => `- **${q.action}**: ${q.result}`).join("\n")}\n\n${result.reports.map((r) => `### ${r.path}\n\n${r.content}`).join("\n\n")}\n\n${result.findings}` : ""}`;
  }
  if (text.startsWith("Native implementation question;")) {
    const parsed = question(text.slice(text.indexOf("{")));
    if (Result.isFailure(parsed)) return text;
    return `### Implementation question · ${parsed.success.assignmentId}\n\n${parsed.success.questions.map((q) => `**${q.question}**\n\n${q.options?.map((o) => `- ${o.label}: ${o.description}`).join("\n") ?? "Custom answer"}`).join("\n\n")}`;
  }
  return text;
}
