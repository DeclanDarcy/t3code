import { Link } from "@tanstack/react-router";
import type { EnvironmentId } from "@t3tools/contracts";
import { useCampaigns, campaignStatusLabel } from "../state/campaigns";
import { useEnvironmentIdentities } from "../state/environments";

function EnvironmentCampaigns({ environmentId }: { environmentId: EnvironmentId }) {
  const { campaigns } = useCampaigns(environmentId);
  if (!campaigns.length) return null;
  return (
    <nav aria-label="Campaigns" className="flex flex-col gap-1 px-2 py-1">
      {campaigns.map((c) => (
        <Link
          key={c.campaignId}
          to="/$environmentId/campaign/$campaignId"
          params={{ environmentId, campaignId: c.campaignId }}
          className="rounded px-2 py-1 text-xs hover:bg-accent"
          activeProps={{ className: "bg-accent" }}
        >
          <div className="truncate font-medium">{c.title}</div>
          <div className="text-muted-foreground">{campaignStatusLabel[c.status]}</div>
        </Link>
      ))}
    </nav>
  );
}
export function CampaignNavigation() {
  const environments = useEnvironmentIdentities();
  return environments.map((e) => (
    <EnvironmentCampaigns key={e.environmentId} environmentId={e.environmentId} />
  ));
}
