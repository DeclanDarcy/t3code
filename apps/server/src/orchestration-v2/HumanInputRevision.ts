import { type ExperimentHumanGuard, type ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export const readHumanInputRevision = Effect.fn("HumanInputRevision.read")(function* (
  threadId: ThreadId,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ sequence: number }>`SELECT COALESCE(MAX(sequence), 0) AS sequence
    FROM orchestration_events
    WHERE aggregate_kind = 'thread' AND stream_id = ${threadId} AND application_event_version = 2
      AND event_type = 'message.updated' AND json_extract(payload_json, '$.createdBy') = 'user'`;
  return { threadId, sequence: rows[0]?.sequence ?? 0 };
});

export const humanInputIsCurrent = Effect.fn("HumanInputRevision.isCurrent")(function* (
  guard: ExperimentHumanGuard,
) {
  for (const expected of guard) {
    if ((yield* readHumanInputRevision(expected.threadId)).sequence !== expected.sequence)
      return false;
  }
  return true;
});
