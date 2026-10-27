import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE experiment_handoffs (
    assignment_id TEXT PRIMARY KEY,
    cognitive_thread_id TEXT NOT NULL,
    implementation_thread_id TEXT NOT NULL,
    closed INTEGER NOT NULL DEFAULT 0,
    payload_json TEXT NOT NULL
  )`;
  yield* sql`CREATE UNIQUE INDEX experiment_handoffs_active_implementation
    ON experiment_handoffs(implementation_thread_id) WHERE closed = 0`;
  yield* sql`CREATE UNIQUE INDEX experiment_handoffs_active_cognitive
    ON experiment_handoffs(cognitive_thread_id) WHERE closed = 0`;
});
