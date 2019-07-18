package aecor.journal.postgres

import aecor.data.TagConsumer
import aecor.journal.postgres.CommittablePostgresEventJournalQueries.OffsetStore
import cats.implicits._
import doobie.Update0
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.fragment.Fragment

final class PostgresOffsetStore(tableName: String) extends OffsetStore[ConnectionIO] {
  def createTable: ConnectionIO[Unit] =
    for {
      _ <- Update0(
        s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          tag TEXT NOT NULL,
          consumer_id TEXT NOT NULL,
          consumer_offset BIGINT NOT NULL
        )
        """,
        none
      ).run
      _ <- Update0(
        s"CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_tag_consumer_id_uidx ON $tableName (tag, consumer_id)",
        none
      ).run
    } yield ()

  private[postgres] def dropTable: ConnectionIO[Unit] =
    Update0(s"DROP TABLE $tableName", none).run.void

  override def setValue(key: TagConsumer, value: Offset): ConnectionIO[Unit] =
    (fr"INSERT INTO"
      ++ Fragment.const(tableName)
      ++ fr"(tag, consumer_id, consumer_offset) VALUES (${key.tag.value}, ${key.consumerId.value}, ${value.value})"
      ++ fr"ON CONFLICT (tag, consumer_id) DO UPDATE SET consumer_offset = EXCLUDED.consumer_offset").update.run.void

  override def getValue(key: TagConsumer): ConnectionIO[Option[Offset]] =
    (fr"SELECT consumer_offset FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE tag = ${key.tag.value} AND consumer_id = ${key.consumerId.value} LIMIT 1")
      .query[Long]
      .option
      .map(_.map(Offset(_)))

  override def deleteValue(key: TagConsumer): ConnectionIO[Unit] =
    (fr"DELETE FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE tag = ${key.tag.value} AND consumer_id = ${key.consumerId.value}").update.run.void
}

object PostgresOffsetStore {
  def apply(tableName: String): PostgresOffsetStore =
    new PostgresOffsetStore(tableName)
}
