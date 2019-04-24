package aecor.journal.postgres

import aecor.encoding.KeyEncoder
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.runtime.Eventsourced.Versioned
import aecor.runtime.KeyValueStore
import cats.Functor
import cats.implicits._
import doobie._
import doobie.free.connection.ConnectionIO
import doobie.implicits._

final class PostgresSnapshotStore[K, S](tableName: String)(implicit S: Serializer[S], encodeKey: KeyEncoder[K])
    extends KeyValueStore[ConnectionIO, K, Versioned[S]] { outer =>

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

  override def setValue(key: K, versioned: Versioned[S]): ConnectionIO[Unit] = {
    val (typeHint, payload) = S.serialize(versioned.value)
    (fr"INSERT INTO" ++ Fragment.const(tableName) ++
      fr"(key, version, type_hint, payload) VALUES (${encodeKey(key)}, ${versioned.version}, $typeHint, $payload)" ++
      fr"ON CONFLICT (key) DO UPDATE SET version = EXCLUDED.version, type_hint = EXCLUDED.type_hint, payload = EXCLUDED.payload").update.run.void
  }
  override def getValue(key: K): ConnectionIO[Option[Versioned[S]]] =
    (fr"SELECT version, type_hint, payload FROM" ++
      Fragment.const(tableName) ++
      fr"WHERE key = ${encodeKey(key)}")
      .query[(Long, String, Array[Byte])]
      .option
      .map {
        case Some((version, typeHint, payload)) =>
          S.deserialize(typeHint, payload) match {
            case Right(s) => Versioned(version, s).some
            case Left(_)  => none
          }
        case None => none
      }

  override def deleteValue(key: K): ConnectionIO[Unit] =
    (fr"DELETE FROM" ++ Fragment.const(tableName) ++
      fr"WHERE key = ${KeyEncoder[K].apply(key)}").update.run.void

  def optional: KeyValueStore[ConnectionIO, K, Versioned[Option[S]]] =
    new OptionalKeyValueStore(this)
}

final class OptionalKeyValueStore[F[_]: Functor, K, S](outer: KeyValueStore[F, K, Versioned[S]])
    extends KeyValueStore[F, K, Versioned[Option[S]]] {
  override def setValue(key: K, versioned: Versioned[Option[S]]): F[Unit] =
    versioned.value match {
      case Some(s) => outer.setValue(key, Versioned(versioned.version, s))
      case None    => deleteValue(key)
    }

  override def getValue(key: K): F[Option[Versioned[Option[S]]]] =
    outer.getValue(key).map {
      case Some(v) => Versioned(v.version, v.value.some).some
      case None    => none
    }

  override def deleteValue(key: K): F[Unit] =
    outer.deleteValue(key)
}

object PostgresSnapshotStore {
  def apply[K, S](tableName: String)(implicit S: Serializer[S], encodeKey: KeyEncoder[K]): PostgresSnapshotStore[K, S] =
    new PostgresSnapshotStore(tableName)
}
