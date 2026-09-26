import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20260926002438_project-association",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`project_association\` (
          \`directory\` text PRIMARY KEY,
          \`project_id\` text NOT NULL,
          \`strategy\` text,
          \`time_created\` integer NOT NULL,
          CONSTRAINT \`fk_project_association_project_id_project_id_fk\` FOREIGN KEY (\`project_id\`) REFERENCES \`project\`(\`id\`) ON DELETE CASCADE
        );
      `)
      yield* tx.run(`CREATE INDEX \`project_association_project_id_idx\` ON \`project_association\` (\`project_id\`);`)
      yield* tx.run(`
        INSERT OR REPLACE INTO \`project_association\` (\`directory\`, \`project_id\`, \`strategy\`, \`time_created\`)
        SELECT \`directory\`, \`project_id\`, \`strategy\`, \`time_created\`
        FROM \`project_directory\`
        WHERE \`type\` = 'association';
      `)
      yield* tx.run(`
        DELETE FROM \`worktree\`
        WHERE (\`project_id\`, \`directory\`) IN (
          SELECT \`project_id\`, \`directory\` FROM \`project_directory\` WHERE \`type\` = 'association'
        );
      `)
      yield* tx.run(`DELETE FROM \`project_directory\` WHERE \`type\` = 'association';`)
    })
  },
}

export default migration
