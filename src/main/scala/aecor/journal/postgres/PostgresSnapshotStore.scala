package aecor.journal.postgres

import aecor.encoding.KeyEncoder
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.runtime.Eventsourced.InternalState
import aecor.runtime.KeyValueStore
import cats.implicits._
import doobie._
import doobie.free.connection.ConnectionIO
import doobie.implicits._

final class PostgresSnapshotStore[K, S](tableName: String)(
    implicit S: Serializer[S],
    encodeKey: KeyEncoder[K])
    extends KeyValueStore[ConnectionIO, K, InternalState[S]] { outer =>

  def createTable: ConnectionIO[Unit] =
    Update0(
      s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          key TEXT PRIMARY KEY,
          version BIGINT NOT NULL,
          type_hint TEXT NOT NULL,
          payload BYTEA NOT NULL
        )
        """,
      none
    ).run.void

  def dropTable: ConnectionIO[Unit] =
    Update0(s"DROP TABLE $tableName", none).run.void

  override def setValue(key: K, value: InternalState[S]): ConnectionIO[Unit] = {
    val (typeHint, payload) = S.serialize(value.entityState)
    (fr"INSERT INTO" ++ Fragment.const(tableName) ++
      fr"(key, version, type_hint, payload) VALUES (${encodeKey(key)}, ${value.version}, $typeHint, $payload)" ++
      fr"ON CONFLICT (key) DO UPDATE SET version = EXCLUDED.version, type_hint = EXCLUDED.type_hint, payload = EXCLUDED.payload").update.run.void
  }
  override def getValue(key: K): ConnectionIO[Option[InternalState[S]]] =
    (fr"SELECT version, type_hint, payload FROM" ++
      Fragment.const(tableName) ++
      fr"WHERE key = ${encodeKey(key)}")
      .query[(Long, String, Array[Byte])]
      .option
      .map {
        case Some((version, typeHint, payload)) =>
          S.deserialize(typeHint, payload) match {
            case Right(s) => InternalState(s, version).some
            case Left(_)  => none
          }
        case None => none
      }

  override def deleteValue(key: K): ConnectionIO[Unit] =
    (fr"DELETE FROM" ++ Fragment.const(tableName) ++
      fr"WHERE key = ${KeyEncoder[K].apply(key)}").update.run.void

  def optional: KeyValueStore[ConnectionIO, K, InternalState[Option[S]]] =
    new KeyValueStore[ConnectionIO, K, InternalState[Option[S]]] {
      override def setValue(key: K, value: InternalState[Option[S]]): ConnectionIO[Unit] =
        value.entityState match {
          case Some(s) => outer.setValue(key, InternalState(s, value.version))
          case None => deleteValue(key)
        }

      override def getValue(key: K): ConnectionIO[Option[InternalState[Option[S]]]] =
        outer.getValue(key).map {
          case Some(s) => InternalState(s.entityState.some, s.version).some
          case None => none
        }

      override def deleteValue(key: K): ConnectionIO[Unit] =
        outer.deleteValue(key)
    }
}

object PostgresSnapshotStore {
  def apply[K, S](tableName: String)(
      implicit S: Serializer[S],
      encodeKey: KeyEncoder[K]): PostgresSnapshotStore[K, S] =
    new PostgresSnapshotStore(tableName)
}
