package aecor.journal.postgres

import aecor.data._
import aecor.encoding.KeyDecoder
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.KeyValueStore
import cats.effect.Temporal
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

final class PostgresEventJournalQueries[F[_]: Temporal, K, E] private[aecor] (
  tableName: String,
  serializer: Serializer[E],
  pollInterval: FiniteDuration,
  xa: Transactor[F]
)(implicit decodeKey: KeyDecoder[K]) {

  implicit val keyRead: Read[K] =
    Read[String].map(s => decodeKey(s).getOrElse(throw new Exception("Failed to decode key")))

  /**
   * Streams all existing events tagged with tag, starting from offset exclusive,
   * sleeps for pollInterval and then streams events starting with latest seen offset
   * @param tag - tag to be used as a filter
   * @param offset - offset to start from, exclusive
   * @return - a stream of events which terminates when reaches the last existing event.
   */
  def eventsByTag(tag: EventTag, offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] = {
    val sleep = Stream.sleep_(pollInterval)
    currentEventsByTag(tag, offset).zipWithNext.noneTerminate
      .flatMap {
        case Some((x, Some(_))) =>
          Stream.emit(x)
        case Some((x @ (latestOffset, _), None)) =>
          Stream.emit(x) ++
            sleep ++
            eventsByTag(tag, latestOffset)
        case None =>
          sleep ++
            eventsByTag(tag, offset)
      }
  }

  /**
   * Streams all existing events tagged with tag, starting from offset exclusive
   * @param tag - tag to be used as a filter
   * @param offset - offset to start from, exclusive
   * @return - a stream of events which terminates when reaches the last existing event.
   */
  def currentEventsByTag(tag: EventTag, offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] =
    (fr"SELECT id, key, seq_nr, type_hint, bytes FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE array_position(tags, ${tag.value} :: text) IS NOT NULL AND (id > ${offset.value}) ORDER BY id ASC")
      .query[(Offset, K, Long, TypeHint, Array[Byte])]
      .stream
      .evalMap { case (eventOffset, key, seqNr, typeHint, bytes) =>
        WeakAsyncConnectionIO
          .fromEither(serializer.deserialize(typeHint, bytes))
          .map { a =>
            (eventOffset, EntityEvent(key, seqNr, a))
          }
      }
      .transact(xa)

  def withOffsetStore(
    offsetStore: KeyValueStore[F, TagConsumer, Offset]
  ): CommittablePostgresEventJournalQueries[F, K, E] =
    new CommittablePostgresEventJournalQueries(this, offsetStore)
}
