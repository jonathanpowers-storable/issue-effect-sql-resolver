import { DevTools } from "@effect/experimental"
import { FileSystem } from "@effect/platform"
import { NodeFileSystem, NodeRuntime } from "@effect/platform-node"
import { SqlClient, SqlResolver } from "@effect/sql"
import { SqliteClient } from "@effect/sql-sqlite-node"
import { Effect, Layer, Schema } from "effect"

const Organization = Schema.Struct({
  id: Schema.Number,
  name: Schema.String,
  address: Schema.String
})

class Facility extends Schema.Class<Facility>("Facility")({
  id: Schema.Number,
  organizationId: Schema.Number,
  name: Schema.String,
  location: Schema.String,
  capacity: Schema.Number
}) {}

const program = Effect.gen(function*() {
  const sql = yield* SqlClient.SqlClient

  yield* Effect.all([
    sql`DROP TABLE IF EXISTS facility;`,
    sql`DROP TABLE IF EXISTS organization`,
    sql`
      CREATE TABLE facility (
        id INTEGER PRIMARY KEY,
        organizationId INTEGER NOT NULL,
        name TEXT NOT NULL,
        location TEXT NOT NULL,
        capacity INTEGER
      );
    `,
    sql` 
      CREATE TABLE organization (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        address TEXT NOT NULL
      );
    `,
    sql`
      INSERT INTO organization (id, name, address) VALUES
      (1, 'Organization One', '123 Main St'),
      (2, 'Organization Two', '456 Elm St');
    `,
    sql`
      INSERT INTO facility (id, organizationId, name, location, capacity) VALUES
      (1, 1, 'Facility A', 'Location A', 100),
      (2, 1, 'Facility B', 'Location B', 200),
      (3, 2, 'Facility C', 'Location C', 150);
    `
  ]).pipe(sql.withTransaction)

  const FindById = yield* SqlResolver.findById(`Organization/findById`, {
    Id: Schema.Number,
    Result: Organization,
    ResultId: (_) => _.id,
    execute: (ids) => sql`SELECT * FROM organization WHERE id IN ${sql.in(ids)};`
  })

  const organizations = yield* sql<Facility>`SELECT * FROM facility;`.pipe(
    Effect.flatMap((rows) =>
      Effect.forEach(
        rows,
        (facility) => FindById.execute(facility.organizationId),
        { batching: true, concurrency: "unbounded" }
      )
    )
  )

  yield* Effect.log(organizations)
})

const SqliteLive = Layer.unwrapScoped(Effect.gen(function*() {
  const fs = yield* FileSystem.FileSystem
  const dir = yield* fs.makeTempDirectoryScoped()
  return SqliteClient.layer({
    filename: dir + "/test.db"
  })
})).pipe(Layer.provide([NodeFileSystem.layer]))

program.pipe(
  Effect.provide([
    SqliteLive,
    DevTools.layer()
  ]),
  Effect.tapErrorCause(Effect.logError),
  NodeRuntime.runMain
)
