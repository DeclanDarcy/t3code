import { createContext, use } from "react";
import type { ChatComposerHandle } from "./components/chat/ChatComposer";

export type ComposerHandleRef = React.RefObject<ChatComposerHandle | null>;

export const ComposerHandleContext = createContext<ComposerHandleRef | null>(null);
export const ComposerHandleActivationContext = createContext<
  (handle: ChatComposerHandle | null, previous?: ChatComposerHandle | null) => void
>(() => {});

export function useComposerHandleContext(): ComposerHandleRef | null {
  return use(ComposerHandleContext);
}
