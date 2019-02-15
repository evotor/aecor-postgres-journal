package aecor.journal.postgres

import cats.implicits.none
import doobie.{ ConnectionIO, Update0 }
import cats.implicits._

final case class Schema(tableName: String) {
  def createTable: ConnectionIO[Unit] =
    for {
      _ <- Update0(s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          id BIGSERIAL,
          key TEXT NOT NULL,
          seq_nr INTEGER NOT NULL CHECK (seq_nr > 0),
          type_hint TEXT NOT NULL,
          bytes BYTEA NOT NULL,
          tags TEXT[] NOT NULL
        )
        """, none).run

      _ <- Update0(
            s"CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_id_uindex ON $tableName (id)",
            none
          ).run

      _ <- Update0(
            s"CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_key_seq_nr_uindex ON $tableName (key, seq_nr)",
            none
          ).run

      _ <- Update0(s"CREATE INDEX IF NOT EXISTS ${tableName}_tags ON $tableName (tags)", none).run
    } yield ()

  def dropTable: ConnectionIO[Unit] =
    Update0(s"DROP TABLE $tableName", none).run.void
}
