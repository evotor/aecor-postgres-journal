package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.EventJournal
import cats.data.NonEmptyChain
import cats.implicits.{none, _}
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.Stream

final class PostgresEventJournalCIO[K, E](tableName: String,
                                          tagging: Tagging[K],
                                          serializer: Serializer[E])(
    implicit
    encodeKey: KeyEncoder[K],
    decodeKey: KeyDecoder[K])
    extends EventJournal[ConnectionIO, K, E] {

  implicit val keyWrite: Write[K] = Write[String].contramap(encodeKey(_))
    implicit val keyRead: Read[K] = Read[String].map(s =>
    decodeKey(s).getOrElse(throw new Exception("Failed to decode key")))


  def createTable: ConnectionIO[Unit] =
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

  def dropTable: ConnectionIO[Unit] =
    Update0(s"DROP TABLE $tableName", none).run.void

  private val appendQuery =
    s"INSERT INTO $tableName (key, seq_nr, type_hint, bytes, tags) VALUES (?, ?, ?, ?, ?)"

  override def append(entityKey: K,
                      offset: Long,
                      events: NonEmptyChain[E]): ConnectionIO[Unit] = {

    type Row = (K, Long, String, Array[Byte], List[String])

    val tags = tagging.tag(entityKey).map(_.value).toList

    def toRow(e: E, idx: Int): Row = {
      val (typeHint, bytes) = serializer.serialize(e)
      (entityKey, idx + offset, typeHint, bytes, tags)
    }

    val toRow_ = (toRow _).tupled

    def insertOne =
      Update[Row](appendQuery).run(toRow(events.head, 0))

    def insertMany =
      Update[Row](appendQuery)
        .updateMany(events.zipWithIndex.map(toRow_))

    val lockTags = tags.traverse_(
      t =>
        sql"select pg_advisory_xact_lock(${t.hashCode})"
          .query[Unit]
          .option)

    val insert =
      if (events.tail.isEmpty) insertOne
      else insertMany

    lockTags >>
      insert.void
  }

  private val deserialize_ =
    (serializer.deserialize _).tupled

  override def foldById[S](key: K, offset: Long, zero: S)(
      f: (S, E) => Folded[S]): ConnectionIO[Folded[S]] =
    (fr"/*NO LOAD BALANCE*/"
      ++ fr"SELECT type_hint, bytes FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE key = ${encodeKey(key)} and seq_nr >= $offset ORDER BY seq_nr ASC")
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

  def currentEventsByTag(
      tag: EventTag,
      offset: Offset): Stream[ConnectionIO, (Offset, EntityEvent[K, E])] =
    (fr"SELECT id, key, seq_nr, type_hint, bytes FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE array_position(tags, ${tag.value} :: text) IS NOT NULL AND (id > ${offset.value}) ORDER BY id ASC")
      .query[(Long, K, Long, String, Array[Byte])]
      .stream
      .map {
        case (eventOffset, key, seqNr, typeHint, bytes) =>
          serializer.deserialize(typeHint, bytes).map { a =>
            (Offset(eventOffset), EntityEvent(key, seqNr, a))
          }
      }
      .evalMap(AsyncConnectionIO.fromEither)
}

object PostgresEventJournalCIO {
  def apply[K: KeyEncoder: KeyDecoder, E](
      tableName: String,
      tagging: Tagging[K],
      serializer: Serializer[E]): PostgresEventJournalCIO[K, E] =
    new PostgresEventJournalCIO(tableName, tagging, serializer)
}
