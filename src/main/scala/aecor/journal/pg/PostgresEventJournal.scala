package aecor.journal.pg

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.pg.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.pg.PostgresEventJournal.{EntityName, Serializer}
import aecor.testkit.EventJournal
import cats.data.NonEmptyVector
import cats.effect.{Async, Timer}
import cats.implicits._
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2._
import doobie.postgres.implicits._

import scala.concurrent.duration.FiniteDuration
/*
CREATE TABLE public.events
(
globalSeqNr SERIAL NOT NULL,
entity TEXT NOT NULL,
key TEXT NOT NULL,
seqNr INTEGER NOT NULL,
typeHint TEXT NOT NULL,
bytes BINARY NOT NULL,
CONSTRAINT entity_events__pk PRIMARY KEY (entity, key, seqNr)
);
CREATE UNIQUE INDEX events_globalSeqNr_uindex ON public.events (globalSeqNr);
CREATE UNIQUE INDEX events_entityEvent_uindex ON public.events (entity, key, seqNr)
 */

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

  final case class ConnectionSettings(host: String,
                                      port: Int,
                                      databaseName: String,
                                      tableName: String,
                                      user: String,
                                      password: String)
  final case class Settings(connectionSettings: ConnectionSettings,
                            pollingInterval: FiniteDuration)

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

  private val appendQuery =
    s"insert into ${connectionSettings.tableName} (entity, key, seqNr, typeHint, bytes, tags) values (?, ?, ?, ?, ?, ?)"

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

    def insertOne(event: E) =
      Update[Row](appendQuery).run(toRow(event, 0))

    def insertAll =
      Update[Row](appendQuery)
        .updateMany(events.zipWithIndex.map(toRow_))

    val cio =
      if (events.tail.isEmpty) insertOne(events.head)
      else
        insertAll

    cio.void.transact(xa)
  }

  private val deserializeF_ =
    (serializer.deserialize _).tupled.andThen(Async[ConnectionIO].fromEither)

  override def foldById[S](key: K, offset: Long, zero: S)(
      f: (S, E) => Folded[S]): F[Folded[S]] =
    (fr"select typeHint, bytes from"
      ++ Fragment.const(connectionSettings.tableName)
      ++ fr"where entity = ${entityName.value} and key = ${encodeKey(key)} and seqNr > $offset ORDER BY seqNr ASC")
      .query[(TypeHint, Array[Byte])]
      .stream
      .evalMap(deserializeF_)
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
    (fr"SELECT globalSeqNr, key, seqNr, typeHint, bytes FROM"
      ++ Fragment.const(connectionSettings.tableName)
      ++ fr"WHERE entity = $entityName AND array_position(tags, ${tag.value} :: text) IS NOT NULL AND (globalSeqNr > $offset) ORDER BY globalSeqNr ASC")
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

}
