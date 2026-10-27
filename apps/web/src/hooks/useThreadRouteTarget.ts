import type { ScopedThreadRef } from "@t3tools/contracts";
import { useParams } from "@tanstack/react-router";
import { useCampaignPaneStore } from "../campaignPaneStore";

import {
  resolveThreadRouteTarget,
  resolveCampaignPaneTarget,
  type ThreadRouteTarget,
} from "../threadRoutes";

function useThreadRouteIds() {
  const environmentId = useParams({ strict: false, select: (params) => params.environmentId });
  const threadId = useParams({ strict: false, select: (params) => params.threadId });
  return { environmentId, threadId };
}

export function useThreadRouteTarget(includeCampaign = true): ThreadRouteTarget | null {
  const { environmentId, threadId } = useThreadRouteIds();
  const draftId = useParams({ strict: false, select: (params) => params.draftId });
  const campaignId = useParams({ strict: false, select: (params) => params.campaignId });
  const focused = useCampaignPaneStore((s) => s.target);
  return (
    resolveThreadRouteTarget({ environmentId, threadId, draftId }) ??
    (includeCampaign ? resolveCampaignPaneTarget({ environmentId, campaignId }, focused) : null)
  );
}

export function useThreadRouteRef(): ScopedThreadRef | null {
  const target = useThreadRouteTarget();
  return target?.kind === "server" ? target.threadRef : null;
}
