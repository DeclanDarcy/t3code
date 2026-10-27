import { WS_METHODS, type EnvironmentId, type CampaignSummary } from "@t3tools/contracts";
import {
  createEnvironmentRpcCommand,
  createEnvironmentRpcSubscriptionAtomFamily,
} from "@t3tools/client-runtime/state/runtime";
import { connectionAtomRuntime } from "../connection/runtime";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import * as Option from "effect/Option";

export const campaignStatusLabel: Record<CampaignSummary["status"], string> = {
  initializing: "Initializing",
  planning: "Cognitive planning",
  implementing: "Implementation running",
  question: "Question mediation",
  reconciling: "Cognitive reconciliation",
  satisfied: "Completed successfully",
  dead_end: "Justified dead-end",
  blocked: "Operationally blocked",
};

export const createCampaign = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "create campaign",
  tag: WS_METHODS.campaignsCreate,
});
export const retryCampaign = createEnvironmentRpcCommand(connectionAtomRuntime, {
  label: "retry campaign initialization",
  tag: WS_METHODS.campaignsRetry,
});
const campaignState = createEnvironmentRpcSubscriptionAtomFamily(connectionAtomRuntime, {
  label: "campaigns",
  tag: WS_METHODS.campaignsSubscribe,
});
export function useCampaigns(environmentId: EnvironmentId) {
  const state = useAtomValue(campaignState({ environmentId, input: {} }));
  return { campaigns: Option.getOrElse(AsyncResult.value(state), () => []), state };
}
