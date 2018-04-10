package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.postgres.PostgresEventJournal.{EntityName, Serializer}
import aecor.runtime.{EventJournal, KeyValueStore}
import cats.data.NonEmptyVector
import cats.effect.{Async, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2._
import doobie.postgres.implicits._

import scala.concurrent.duration.FiniteDuration

object PostgresEventJournal extends OffsetStoreSupport {
  trait Serializer[A] {
    def serialize(a: A): (TypeHint, Array[Byte])
    def deserialize(typeHint: TypeHint,
                    bytes: Array[Byte]): Either[Throwable, A]
  }
  object Serializer {
    type TypeHint = String
  }
  final case class EntityName(value: String) extends AnyVal
  object EntityName {
    implicit val composite: Composite[EntityName] =
      Composite[String].imap(EntityName(_))(_.value)
  }

  final case class Settings(connectionSettings: Settings.Connection,
                            pollingInterval: FiniteDuration)
  object Settings {
    final case class Connection(host: String,
                                port: Int,
                                databaseName: String,
                                tableName: String,
                                user: String,
                                password: String)
  }

  final class Builder[F[_]] {
    def apply[K, E](settings: PostgresEventJournal.Settings,
                    entityName: EntityName,
                    tagging: Tagging[K],
                    serializer: Serializer[E])(
        implicit F: Async[F],
        encodeKey: KeyEncoder[K],
        decodeKey: KeyDecoder[K],
        timer: Timer[F]): PostgresEventJournal[F, K, E] =
      new PostgresEventJournal(settings, entityName, tagging, serializer)
  }

  def apply[F[_]]: Builder[F] = new Builder[F]

}

final class PostgresEventJournal[F[_], K, E](
    settings: PostgresEventJournal.Settings,
    entityName: EntityName,
    tagging: Tagging[K],
    serializer: Serializer[E])(implicit F: Async[F],
                               encodeKey: KeyEncoder[K],
                               decodeKey: KeyDecoder[K],
                               timer: Timer[F])
    extends EventJournal[F, K, E] {
  import settings._

  implicit val keyComposite: Composite[K] = Composite[String].imap(s =>
    decodeKey(s).getOrElse(throw new Exception("")))(encodeKey(_))

  private val xa = Transactor.fromDriverManager[F](
    "org.postgresql.Driver",
    s"jdbc:postgresql://${connectionSettings.host}:${connectionSettings.port}/${connectionSettings.databaseName}",
    connectionSettings.user,
    connectionSettings.password
  )

  def createTable: F[Unit] = xa.trans.apply {
    for {
      _ <- Update0(
        s"""
        CREATE TABLE IF NOT EXISTS ${connectionSettings.tableName} (
          id BIGSERIAL,
          entity TEXT NOT NULL,
          key TEXT NOT NULL,
          seq_nr INTEGER NOT NULL CHECK (seq_nr > 0),
          type_hint TEXT NOT NULL,
          bytes BYTEA NOT NULL,
          tags TEXT[] NOT NULL
        )
        """,
        none
      ).run

      _ <- Update0(
        s"CREATE UNIQUE INDEX IF NOT EXISTS ${connectionSettings.tableName}_id_uindex ON ${connectionSettings.tableName} (id)",
        none).run

      _ <- Update0(
        s"CREATE UNIQUE INDEX IF NOT EXISTS ${connectionSettings.tableName}_entity_key_seq_nr_uindex ON ${connectionSettings.tableName} (entity, key, seq_nr)",
        none
      ).run
    } yield ()
  }

  private[postgres] def dropTable: F[Unit] =
    Update0(s"DROP TABLE ${connectionSettings.tableName}", none).run
      .transact(xa)
      .void

  private val appendQuery =
    s"INSERT INTO ${connectionSettings.tableName} (entity, key, seq_nr, type_hint, bytes, tags) VALUES (?, ?, ?, ?, ?, ?)"

  override def append(entityKey: K,
                      offset: Long,
                      events: NonEmptyVector[E]): F[Unit] = {

    type Row = (EntityName, K, Long, String, Array[Byte], List[String])

    def toRow(e: E, idx: Int): Row = {
      val (typeHint, bytes) = serializer.serialize(e)
      (entityName,
       entityKey,
       idx + offset + 1,
       typeHint,
       bytes,
       tagging.tag(entityKey).map(_.value).toList)
    }

    val toRow_ = (toRow _).tupled

    def insertOne =
      Update[Row](appendQuery).run(toRow(events.head, 0))

    def insertMany =
      Update[Row](appendQuery)
        .updateMany(events.zipWithIndex.map(toRow_))

    val cio =
      if (events.tail.isEmpty) insertOne
      else insertMany

    cio.void.transact(xa)
  }

  private val deserialize_ =
    (serializer.deserialize _).tupled

  override def foldById[S](key: K, offset: Long, zero: S)(
      f: (S, E) => Folded[S]): F[Folded[S]] =
    (fr"SELECT type_hint, bytes FROM"
      ++ Fragment.const(connectionSettings.tableName)
      ++ fr"WHERE entity = ${entityName.value} and key = ${encodeKey(key)} and seq_nr > $offset ORDER BY seq_nr ASC")
      .query[(TypeHint, Array[Byte])]
      .stream
      .map(deserialize_)
      .evalMap(AsyncConnectionIO.fromEither)
      .scan(Folded.next(zero))((acc, e) => acc.flatMap(f(_, e)))
      .takeWhile(_.isNext, takeFailure = true)
      .compile
      .last
      .map {
        case Some(x) => x
        case None    => Folded.next(zero)
      }
      .transact(xa)

  def eventsByTag(tag: EventTag,
                  offset: Long): Stream[F, (Long, EntityEvent[K, E])] = {
    currentEventsByTag(tag, offset).zipWithNext
      .flatMap {
        case (x, Some(_)) => Stream.emit(x)
        case (x @ (latestOffset, _), None) =>
          Stream
            .emit(x)
            .append(Stream
              .eval(timer.sleep(pollingInterval)) >> eventsByTag(tag,
                                                                 latestOffset))

      }
      .append(Stream
        .eval(timer.sleep(pollingInterval)) >> eventsByTag(tag, offset))

  }

  def currentEventsByTag(tag: EventTag,
                         offset: Long): Stream[F, (Long, EntityEvent[K, E])] =
    (fr"SELECT id, key, seq_nr, type_hint, bytes FROM"
      ++ Fragment.const(connectionSettings.tableName)
      ++ fr"WHERE entity = $entityName AND array_position(tags, ${tag.value} :: text) IS NOT NULL AND (id > $offset) ORDER BY id ASC")
      .query[(Long, K, Long, String, Array[Byte])]
      .stream
      .transact(xa)
      .map {
        case (eventOffset, key, seqNr, typeHint, bytes) =>
          serializer.deserialize(typeHint, bytes).map { a =>
            (eventOffset, EntityEvent(key, seqNr, a))
          }
      }
      .evalMap(F.fromEither)

  def withOffsetStore(offsetStore: KeyValueStore[F, TagConsumer, Long])
    : PostgresEventJournal.WithOffsetStore[F, K, E] =
    new PostgresEventJournal.WithOffsetStore[F, K, E](this, offsetStore)
}
