import { createContext, use, useLayoutEffect, useRef, type ReactNode } from "react";
import { ComposerHandleContext, ComposerHandleActivationContext } from "../composerHandleContext";
import type { ChatComposerHandle } from "./chat/ChatComposer";

export const ConversationActiveContext = createContext<boolean | null>(null);
export const useConversationActive = () => use(ConversationActiveContext) ?? true;
export const useConversationAutofocus = () => use(ConversationActiveContext) === null;
export const CognitiveConversationContext = createContext(false);

export function CampaignConversationPane({
  active,
  onActivate,
  children,
  cognitive = false,
}: {
  active: boolean;
  onActivate: () => void;
  children: ReactNode;
  cognitive?: boolean;
}) {
  const local = useRef<ChatComposerHandle | null>(null);
  const activateComposer = use(ComposerHandleActivationContext);
  // oxlint-disable-next-line t3code/react-effect-budget -- The palette must address only the focused pane's independent editor.
  useLayoutEffect(() => {
    const handle = local.current;
    if (active) activateComposer(handle);
    return () => {
      activateComposer(null, handle);
    };
  }, [active, activateComposer]);
  const activate = () => {
    activateComposer(local.current);
    onActivate();
  };
  return (
    <div
      className="flex min-h-0 min-w-0 flex-1 flex-col"
      onFocusCapture={activate}
      onPointerDownCapture={activate}
    >
      <ConversationActiveContext value={active}>
        <CognitiveConversationContext value={cognitive}>
          <ComposerHandleContext value={local}>{children}</ComposerHandleContext>
        </CognitiveConversationContext>
      </ConversationActiveContext>
    </div>
  );
}
