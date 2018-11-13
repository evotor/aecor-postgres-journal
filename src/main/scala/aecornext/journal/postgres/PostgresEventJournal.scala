package aecornext.journal.postgres

import aecornext.data._
import aecornext.encoding.{KeyDecoder, KeyEncoder}
import aecornext.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecornext.journal.postgres.PostgresEventJournal.Serializer
import aecornext.runtime.EventJournal
import cats.data.NonEmptyChain
import cats.effect.{Async, Timer}
import cats.implicits.{none, _}
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.Stream
import Meta.StringMeta
import scala.reflect.runtime.universe.TypeTag
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

  final case class Settings(tableName: String, pollingInterval: FiniteDuration)

  def apply[F[_]: Timer: Async, K: KeyEncoder: KeyDecoder: TypeTag, E](
      transactor: Transactor[F],
      settings: PostgresEventJournal.Settings,
      tagging: Tagging[K],
      serializer: Serializer[E]): PostgresEventJournal[F, K, E] =
    new PostgresEventJournal(transactor, settings, tagging, serializer)

}

final class PostgresEventJournal[F[_], K, E](
    xa: Transactor[F],
    settings: PostgresEventJournal.Settings,
    tagging: Tagging[K],
    serializer: Serializer[E])(implicit F: Async[F],
                               encodeKey: KeyEncoder[K],
                               decodeKey: KeyDecoder[K],
                               timer: Timer[F],
                               T: TypeTag[K])
    extends EventJournal[F, K, E]
    with PostgresEventJournalQueries[F, K, E] {
  import settings._

  implicit val keyWrite: Meta[K] = implicitly[Meta[String]].xmap(
    s => decodeKey(s).getOrElse(throw new Exception("Failed to decode key")),
    encodeKey(_)
  )

  private def createTableCIO =
    for {
      _ <- Update0(
        s"""
        CREATE TABLE IF NOT EXISTS $tableName (
          id BIGSERIAL,
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
        s"CREATE UNIQUE INDEX IF NOT EXISTS ${tableName}_key_seq_nr_uindex ON $tableName (key, seq_nr)",
        none
      ).run
    } yield ()

  def createTable: F[Unit] = createTableCIO.transact(xa)

  private[postgres] def dropTable: F[Unit] =
    Update0(s"DROP TABLE $tableName", none).run.void.transact(xa)

  private val appendQuery =
    s"INSERT INTO $tableName (key, seq_nr, type_hint, bytes, tags) VALUES (?, ?, ?, ?, ?)"

  override def append(entityKey: K,
                      offset: Long,
                      events: NonEmptyChain[E]): F[Unit] = {

    type Row = (K, Long, String, Array[Byte], List[String])

    def toRow(e: E, idx: Int): Row = {
      val (typeHint, bytes) = serializer.serialize(e)
      (entityKey,
       idx + offset,
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
      ++ fr"WHERE key = ${encodeKey(key)} and seq_nr >= $offset ORDER BY seq_nr ASC FOR UPDATE")
      .query[(TypeHint, Array[Byte])]
      .stream
      .transact(xa)
      .map(deserialize_)
      .evalMap(F.fromEither)
      .scan(Folded.next(zero))((acc, e) => acc.flatMap(f(_, e)))
      .takeWhile(_.isNext, takeFailure = true)
      .compile
      .last
      .map {
        case Some(x) => x
        case None    => Folded.next(zero)
      }

  override protected val sleepBeforePolling: F[Unit] =
    timer.sleep(pollingInterval)

  /**
    * Streams all existing events tagged with tag, starting from offset exclusive
    * @param tag - tag to be used as a filter
    * @param offset - offset to start from, exclusive
    * @return - a stream of events which terminates when reaches the last existing event.
    */
  def currentEventsByTag(
      tag: EventTag,
      offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] =
    (fr"SELECT id, key, seq_nr, type_hint, bytes FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE array_position(tags, ${tag.value} :: text) IS NOT NULL AND (id > ${offset.value}) ORDER BY id ASC")
      .query[(Long, K, Long, String, Array[Byte])]
      .stream
      .transact(xa)
      .map {
        case (eventOffset, key, seqNr, typeHint, bytes) =>
          serializer.deserialize(typeHint, bytes).map { a =>
            (Offset(eventOffset), EntityEvent(key, seqNr, a))
          }
      }
      .evalMap(F.fromEither)

}
