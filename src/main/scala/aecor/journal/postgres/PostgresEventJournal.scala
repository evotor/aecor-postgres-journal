package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.postgres.internal.ConnectionIOPostgresEventJournal
import aecor.runtime.EventJournal
import cats.data.NonEmptyVector
import cats.effect.{IO, Timer}
import cats.~>
import doobie._
import fs2._

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.postgres.PostgresEventJournal.{EntityName, Serializer}
import cats.data.NonEmptyVector
import cats.effect.{IO, Timer}
import cats.implicits.{none, _}
import doobie.postgres.implicits._
import doobie._
import doobie.implicits._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

object PostgresEventJournal {
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

  final case class Settings(tableName: String, pollingInterval: FiniteDuration)

  def apply[F[_], K, E](transactor: Transactor[F],
                        settings: PostgresEventJournal.Settings,
                        entityName: EntityName,
                        tagging: Tagging[K],
                        serializer: Serializer[E])(
      implicit
      encodeKey: KeyEncoder[K],
      decodeKey: KeyDecoder[K],
      timer: Timer[F]): PostgresEventJournal[F, K, E] =
    new PostgresEventJournal(transactor,
                             settings,
                             entityName,
                             tagging,
                             serializer)

}

final class PostgresEventJournal[F[_], K, E](
    xa: Transactor[F],
    settings: PostgresEventJournal.Settings,
    entityName: EntityName,
    tagging: Tagging[K],
    serializer: Serializer[E])(implicit
                               encodeKey: KeyEncoder[K],
                               decodeKey: KeyDecoder[K],
                               timer: Timer[F])
    extends EventJournal[F, K, E]
    with PostgresEventJournalQueries[F, K, E] {
  import settings._

  implicit val keyComposite: Composite[K] = Composite[String].imap(s =>
    decodeKey(s).getOrElse(throw new Exception("")))(encodeKey(_))

  private def createTableCIO =
    for {
      _ <- Update0(
        s"""
        CREATE TABLE IF NOT EXISTS $tableName (
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
        s"CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_id_uindex ON $tableName (id)",
        none).run

      _ <- Update0(
        s"CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_entity_key_seq_nr_uindex ON $tableName (entity, key, seq_nr)",
        none
      ).run
    } yield ()

  def createTable: F[Unit] = createTableCIO.transact(xa)

  private[postgres] def dropTable: F[Unit] =
    Update0(s"DROP TABLE $tableName", none).run.void.transact(xa)

  private val appendQuery =
    s"INSERT INTO $tableName (entity, key, seq_nr, type_hint, bytes, tags) VALUES (?, ?, ?, ?, ?, ?)"

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
      ++ Fragment.const(tableName)
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

  def currentEventsByTag(tag: EventTag,
                         offset: Long): Stream[F, (Long, EntityEvent[K, E])] =
    (fr"SELECT id, key, seq_nr, type_hint, bytes FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE entity = $entityName AND array_position(tags, ${tag.value} :: text) IS NOT NULL AND (id > $offset) ORDER BY id ASC")
      .query[(Long, K, Long, String, Array[Byte])]
      .stream
      .map {
        case (eventOffset, key, seqNr, typeHint, bytes) =>
          serializer.deserialize(typeHint, bytes).map { a =>
            (eventOffset, EntityEvent(key, seqNr, a))
          }
      }
      .evalMap(AsyncConnectionIO.fromEither)
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

}
