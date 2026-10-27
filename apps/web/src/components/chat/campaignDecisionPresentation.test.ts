import { expect, it } from "vite-plus/test";
import {
  campaignDecisionPresentation,
  campaignRequestPresentation,
} from "./campaignDecisionPresentation";

it("presents validated cognitive prose without promoting partial JSON or arbitrary messages", () => {
  const text = JSON.stringify({
    assignmentId: "assignment:1",
    requestId: "job:1",
    kind: "continue",
    markdown: "## Next experiment\nReal assignment",
    rationale: "Tests demonstrate a remaining gap.",
  });
  expect(campaignDecisionPresentation(text)).toContain("## Next experiment\nReal assignment");
  expect(campaignDecisionPresentation(text)).toContain("Tests demonstrate");
  const invalid = '{"kind":"satisfied"}';
  expect(campaignDecisionPresentation(invalid)).toBe(invalid);
  expect(campaignDecisionPresentation("ordinary conversation")).toBe("ordinary conversation");
});

it("presents readable original input and reports while preserving malformed requests verbatim", () => {
  const request =
    "Experiment decision request.\n" +
    JSON.stringify({
      originalInput: "Café λ\nKeep  spaces.",
      assignmentId: "test:1",
      phase: "reconciliation",
      attachmentEvidence: [{ name: "input.md", text: "Whole attachment" }],
      implementationReport: {
        source: "implementation_agent_submission",
        value: {
          assignmentId: "test:1",
          attemptId: "test:attempt",
          resultId: "test:result",
          disposition: "completed",
          changes: "Actual changes",
          qualification: [{ action: "node --test", result: "18 passed" }],
          reports: [{ path: "report.md", content: "Actual report body" }],
          findings: "Known limits",
        },
      },
    });
  expect(campaignRequestPresentation(request)).toContain("Café λ\nKeep  spaces.");
  expect(campaignRequestPresentation(request)).toContain("Whole attachment");
  expect(campaignRequestPresentation(request)).toContain("18 passed");
  expect(campaignRequestPresentation(request)).toContain("Actual report body");
  const malformed = "Experiment decision request.\n{}";
  expect(campaignRequestPresentation(malformed)).toBe(malformed);
  expect(campaignRequestPresentation("Human ordinary input")).toBe("Human ordinary input");
});
