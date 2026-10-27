import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Result from "effect/Result";
import { ChatAttachmentId, type ChatAttachment } from "@t3tools/contracts";
import { createPendingAttachmentId, resolveAttachmentPath } from "../attachmentStore.ts";
import { readCognitiveAttachmentEvidence } from "./CognitiveAttachmentEvidence.ts";

it.effect("supplies complete cognitive file text and rejects unusable input before inference", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const attachmentsDir = yield* fs.makeTempDirectoryScoped();
    const attachment: ChatAttachment = {
      id: ChatAttachmentId.make(createPendingAttachmentId("md")),
      type: "file",
      name: "input.md",
      mimeType: "text/markdown",
      sizeBytes: 10,
    };
    const path = resolveAttachmentPath({ attachmentsDir, attachment })!;
    const read = () =>
      readCognitiveAttachmentEvidence({
        attachments: [attachment],
        attachmentsDir,
        resolveTextPath: () => Effect.sync(() => undefined),
      });
    const body = "Café λ\nKeep  spaces.\n";
    yield* fs.writeFileString(path, body);
    const evidence = yield* read();
    assert.equal(evidence[0]!.text, body);
    assert.equal(evidence[0]!.sha256.length, 64);
    for (const bytes of [
      new Uint8Array([0xff]),
      new Uint8Array([65, 0, 66]),
      new Uint8Array(48_001).fill(65),
    ]) {
      yield* fs.writeFile(path, bytes);
      assert.isTrue(Result.isFailure(yield* Effect.result(read())));
    }
  }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
);
