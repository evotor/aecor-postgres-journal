package aecor.journal.postgres

import aecor.data.Tagging
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal.Serializer
import cats.Monad
import cats.effect.Timer
import cats.implicits.{none, _}
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import scala.concurrent.duration._
import scala.concurrent.duration.FiniteDuration

final class JournalSchema[K, E](tableName: String, serializer: Serializer[E], trackTimestamps: Boolean)(implicit
  keyEncoder: KeyEncoder[K],
  keyDecoder: KeyDecoder[K]
) {
  def create: ConnectionIO[Unit] =
    for {
      _ <- Update0(
        s"""
        CREATE TABLE IF NOT EXISTS $tableName
          ( id BIGSERIAL
          , key TEXT NOT NULL
          , seq_nr INTEGER NOT NULL CHECK (seq_nr > 0)
          , type_hint TEXT NOT NULL
          , bytes BYTEA NOT NULL
          , tags TEXT[] NOT NULL
          ${if (trackTimestamps) ", timestamp timestamp DEFAULT now()" else ""}
          )
        """,
        none
      ).run

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

  def drop: ConnectionIO[Unit] =
    Update0(s"DROP TABLE $tableName", none).run.void

  def journal(tagging: Tagging[K]): PostgresEventJournal[K, E] =
    PostgresEventJournal(tableName, tagging, serializer)

  def queries[F[_]: Timer: Monad](xa: Transactor[F],
                                  pollInterval: FiniteDuration = 100.millis
  ): PostgresEventJournalQueries[F, K, E] =
    new PostgresEventJournalQueries(tableName, serializer, pollInterval, xa)
}

object JournalSchema {
  def apply[K, E](tableName: String, serializer: Serializer[E], trackTimestamps: Boolean = false)(implicit
    keyEncoder: KeyEncoder[K],
    keyDecoder: KeyDecoder[K]
  ): JournalSchema[K, E] = new JournalSchema(tableName, serializer, trackTimestamps)
}
