import type { ChatAttachment } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Crypto from "effect/Crypto";
import * as Hex from "effect/encoding/Hex";
import * as Schema from "effect/Schema";
import { resolveAttachmentPath } from "../attachmentStore.ts";

class CognitiveAttachmentError extends Schema.TaggedError<CognitiveAttachmentError>()(
  "CognitiveAttachmentError",
  { message: Schema.String },
) {}
const error = (message: string) => new CognitiveAttachmentError({ message });

export const readCognitiveAttachmentEvidence = Effect.fn("CognitiveAttachmentEvidence.read")(
  function* (input: {
    attachments: ReadonlyArray<ChatAttachment>;
    attachmentsDir: string;
    resolveTextPath: (path: string) => Effect.Effect<string | undefined>;
  }) {
    const fs = yield* FileSystem.FileSystem;
    const crypto = yield* Crypto.Crypto;
    const evidence = [];
    let total = 0;
    for (const attachment of input.attachments) {
      if (attachment.type === "image") continue;
      if (attachment.type !== "file")
        return yield* error(
          "This attachment type cannot be supplied to the restricted cognitive agent. Attach readable text, a supported extracted document, or an image.",
        );
      const path = resolveAttachmentPath({ attachmentsDir: input.attachmentsDir, attachment });
      if (!path) return yield* error("Cognitive attachment reference is invalid.");
      const extracted = yield* input.resolveTextPath(path);
      if (
        !extracted &&
        !attachment.mimeType.startsWith("text/") &&
        !/\.(txt|md|markdown|json|jsonl|csv|tsv|xml|yaml|yml|toml|ini|log|js|mjs|cjs|jsx|ts|tsx|py|rs|go|java|c|h|cpp|cs|sh|ps1|sql|html|css|svg)$/i.test(
          attachment.name,
        )
      )
        return yield* error(
          `Attachment '${attachment.name}' has no supported text representation. Supply extracted UTF-8 text or an image; binary files cannot be passed to web-search-only cognition.`,
        );
      const bytes = yield* fs.readFile(extracted ?? path);
      total += bytes.length;
      if (bytes.length > 48_000 || total > 72_000)
        return yield* error(
          "Cognitive text attachments exceed 48 KB per file or 72 KB total. Reduce them explicitly; content is never silently truncated.",
        );
      const decoded = yield* Effect.try({
        try: () => new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        catch: () =>
          error("A cognitive attachment has no supported readable UTF-8 representation."),
      });
      if (decoded.includes("\0"))
        return yield* error("Binary attachment content cannot be supplied as cognitive text.");
      evidence.push({
        attachmentId: attachment.id,
        name: attachment.name,
        sha256: Hex.encode(yield* crypto.digest("SHA-256", bytes)),
        text: decoded,
      });
    }
    return evidence;
  },
);
