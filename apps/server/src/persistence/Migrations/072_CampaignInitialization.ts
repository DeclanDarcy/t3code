import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE campaign_initializations(campaign_id TEXT PRIMARY KEY, state TEXT NOT NULL, claimed INTEGER NOT NULL DEFAULT 0, payload_json TEXT NOT NULL)`;
});
