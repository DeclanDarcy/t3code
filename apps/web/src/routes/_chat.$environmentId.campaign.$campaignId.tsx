import { createFileRoute } from "@tanstack/react-router";
import { EnvironmentId } from "@t3tools/contracts";
import { CampaignRouteView } from "../components/CampaignRouteView";

export const Route = createFileRoute("/_chat/$environmentId/campaign/$campaignId")({
  component: CampaignPage,
});
function CampaignPage() {
  const { environmentId, campaignId } = Route.useParams();
  return (
    <CampaignRouteView
      key={`${environmentId}:${campaignId}`}
      environmentId={EnvironmentId.make(environmentId)}
      campaignId={campaignId}
    />
  );
}
