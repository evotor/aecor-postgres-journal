package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.EventJournal
import cats.data.NonEmptyChain
import cats.effect.{Async, Timer}
import cats.implicits.{none, _}
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
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

  def apply[F[_]: Async, K: KeyEncoder: KeyDecoder, E](
      transactor: Transactor[F],
      tableName: String,
      tagging: Tagging[K],
      serializer: Serializer[E]): PostgresEventJournal[F, K, E] =
    new PostgresEventJournal(transactor, tableName, tagging, serializer)

}

final class PostgresEventJournal[F[_], K, E](xa: Transactor[F],
                                             tableName: String,
                                             tagging: Tagging[K],
                                             serializer: Serializer[E])(
    implicit F: Async[F],
    encodeKey: KeyEncoder[K],
    decodeKey: KeyDecoder[K])
    extends EventJournal[F, K, E] {

  implicit val keyWrite: Write[K] = Write[String].contramap(encodeKey(_))
  implicit val keyRead: Read[K] = Read[String].map(s =>
    decodeKey(s).getOrElse(throw new Exception("Failed to decode key")))

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

   val tags = tagging.tag(entityKey).map(_.value).toList;

    def toRow(e: E, idx: Int): Row = {
      val (typeHint, bytes) = serializer.serialize(e)
      (entityKey,
       idx + offset,
       typeHint,
       bytes,
       tags)
    }

    val toRow_ = (toRow _).tupled

    def insertOne =
      Update[Row](appendQuery).run(toRow(events.head, 0))

    def insertMany =
      Update[Row](appendQuery)
        .updateMany(events.zipWithIndex.map(toRow_))

    val lockQuery = tags.traverse(t => sql"select * from pg_advisory_xact_lock(${t.hashCode})".query[Unit].option)

    val cio =
      if (events.tail.isEmpty) insertOne
      else insertMany

    val lockAndRun =
      lockQuery >>
        cio.void

    lockAndRun.transact(xa)
  }

  private val deserialize_ =
    (serializer.deserialize _).tupled

  override def foldById[S](key: K, offset: Long, zero: S)(
      f: (S, E) => Folded[S]): F[Folded[S]] =
    (fr"/*NO LOAD BALANCE*/"
      ++ fr"SELECT type_hint, bytes FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE key = ${encodeKey(key)} and seq_nr >= $offset ORDER BY seq_nr ASC")
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

  def queries(
      pollingInterval: FiniteDuration)(implicit timer: Timer[F]): PostgresEventJournalQueries[F, K, E] =
    new PostgresEventJournalQueries[F, K, E] {

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

}
