import type { EnvironmentId, ThreadId } from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { useEffect, useState, type CSSProperties } from "react";
import { useCampaignPaneStore } from "../campaignPaneStore";
import { useConnectedEnvironmentIds } from "../state/environments";
import * as Schema from "effect/Schema";
import { useCampaigns, retryCampaign, campaignStatusLabel } from "../state/campaigns";
import { useOrchestrationCommand } from "../state/use-orchestration-command";
import { useLocalStorage } from "../hooks/useLocalStorage";
import { useResizeDrag } from "../hooks/useResizeDrag";
import { SidebarInset } from "./ui/sidebar";
import { Button } from "./ui/button";
import ChatView from "./ChatView";
import { CampaignConversationPane } from "./campaignConversation";

export function CampaignRouteView({
  environmentId,
  campaignId,
}: {
  environmentId: EnvironmentId;
  campaignId: string;
}) {
  const { campaigns, state } = useCampaigns(environmentId);
  const connected = useConnectedEnvironmentIds().includes(environmentId);
  const campaign = campaigns.find((c) => c.campaignId === campaignId);
  const [active, setActive] = useState<"cognitive" | "implementation">("cognitive");
  const [storedSplit, setSplit] = useLocalStorage("campaign-pane-split", 50, Schema.Finite);
  const split = Math.max(30, Math.min(70, storedSplit));
  const retry = useOrchestrationCommand(retryCampaign);
  const cognitiveId = campaign?.cognitiveThreadId;
  const implementationId = campaign?.implementationThreadId;
  // oxlint-disable-next-line t3code/react-effect-budget -- Global thread commands and the palette follow the focused campaign pane.
  useEffect(() => {
    const threadId = active === "cognitive" ? cognitiveId : implementationId;
    if (!threadId) return;
    useCampaignPaneStore.setState({
      target: { campaignId, threadRef: { environmentId, threadId } },
    });
    return () => {
      if (useCampaignPaneStore.getState().target?.campaignId === campaignId)
        useCampaignPaneStore.setState({ target: null });
    };
  }, [active, campaignId, cognitiveId, implementationId, environmentId]);
  const { ref: resizeHandleRef, ...drag } = useResizeDrag<HTMLDivElement>((event) => {
    const host = event.currentTarget.parentElement;
    if (!host) return null;
    const width = host.getBoundingClientRect().width;
    return {
      width: (width * split) / 100,
      edge: "right",
      bounds: { min: width * 0.3, max: width * 0.7 },
      resize(value) {
        host.style.setProperty("--campaign-left", `${(value / width) * 100}%`);
        return value;
      },
      finish(value) {
        setSplit((value / width) * 100);
      },
      cleanup(committed) {
        if (!committed) host.style.setProperty("--campaign-left", `${split}%`);
      },
    };
  });
  const member = (role: "cognitive" | "implementation", id: ThreadId) => (
    <section
      aria-label={`${role} conversation`}
      className={`flex min-h-0 min-w-0 flex-1 flex-col ${role === "cognitive" ? "@[900px]:w-(--campaign-left) @[900px]:flex-none" : ""}`}
    >
      <div className="flex shrink-0 items-center justify-between border-b px-3 py-1 text-xs text-muted-foreground">
        <span>
          {role === "cognitive"
            ? "Cognitive agent · web search only"
            : "Implementation agent · ordinary Codex"}
        </span>
        <Link
          to="/$environmentId/$threadId"
          params={{ environmentId, threadId: id }}
          className="underline"
        >
          Open thread
        </Link>
      </div>
      <CampaignConversationPane
        cognitive={role === "cognitive"}
        active={active === role}
        onActivate={() => setActive(role)}
      >
        <ChatView
          environmentId={environmentId}
          threadId={id}
          routeKind="server"
          reserveTitleBarControlInset={false}
        />
      </CampaignConversationPane>
    </section>
  );
  return (
    <SidebarInset className="h-dvh min-w-0 flex-col">
      <header
        className="flex shrink-0 flex-wrap items-center gap-x-4 gap-y-1 border-b px-4 py-2"
        aria-live="polite"
      >
        <h1 className="min-w-0 truncate text-sm font-medium">{campaign?.title ?? "Campaign"}</h1>
        <span className="text-xs text-muted-foreground" data-testid="campaign-status">
          {!connected ? "Reconnecting · last known: " : ""}
          {campaign
            ? campaignStatusLabel[campaign.status]
            : state._tag === "Success"
              ? "Campaign not found in this environment"
              : "Connecting to campaign…"}
          {campaign?.assignmentNumber ? ` · Experiment ${campaign.assignmentNumber}` : ""}
        </span>
        {campaign?.reason ? (
          <p className="w-full text-xs text-muted-foreground">{campaign.reason}</p>
        ) : null}
        {campaign?.canRetryInitialization ? (
          <Button
            variant="outline"
            size="sm"
            onClick={() => void retry({ environmentId, input: { campaignId } })}
          >
            Retry initialization
          </Button>
        ) : null}
      </header>
      <div className="@container min-h-0 flex-1">
        {campaign && campaign.status !== "initializing" ? (
          <div
            className="flex h-full min-h-0 flex-col @[900px]:flex-row"
            style={{ "--campaign-left": `${split}%` } as CSSProperties}
          >
            {member("cognitive", campaign.cognitiveThreadId)}
            <div
              ref={resizeHandleRef}
              onPointerDown={(event) => drag.onPointerDown(event)}
              onPointerMove={(event) => drag.onPointerMove(event)}
              onPointerUp={(event) => drag.onPointerUp(event)}
              onPointerCancel={(event) => drag.onPointerCancel(event)}
              onLostPointerCapture={(event) => drag.onLostPointerCapture(event)}
              role="separator"
              tabIndex={0}
              aria-label="Resize campaign conversations"
              aria-orientation="vertical"
              aria-valuemin={30}
              aria-valuemax={70}
              aria-valuenow={Math.round(split)}
              onKeyDown={(e) => {
                if (e.key === "ArrowLeft" || e.key === "ArrowRight") {
                  e.preventDefault();
                  setSplit(Math.max(30, Math.min(70, split + (e.key === "ArrowLeft" ? -5 : 5))));
                } else if (e.key === "Home") {
                  e.preventDefault();
                  setSplit(50);
                }
              }}
              className="hidden w-1.5 shrink-0 cursor-col-resize bg-border focus-visible:bg-primary @[900px]:block"
            />
            {member("implementation", campaign.implementationThreadId)}
          </div>
        ) : (
          <p className="p-6 text-sm text-muted-foreground">
            {campaign
              ? "Preparing both conversations. It is safe to navigate away; initialization continues on the server."
              : "Waiting for authoritative campaign state from this environment."}
          </p>
        )}
      </div>
    </SidebarInset>
  );
}
