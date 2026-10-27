import { create } from "zustand";
import type { ScopedThreadRef } from "@t3tools/contracts";

export const useCampaignPaneStore = create<{
  target: { campaignId: string; threadRef: ScopedThreadRef } | null;
}>(() => ({ target: null }));
