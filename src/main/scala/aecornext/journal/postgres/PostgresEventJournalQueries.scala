package aecornext.journal.postgres

import aecornext.data._
import aecornext.runtime.KeyValueStore
import cats.Functor
import cats.implicits._
import fs2.Stream

object PostgresEventJournalQueries {
  type OffsetStore[F[_]] = KeyValueStore[F, TagConsumer, Offset]
  final class WithOffsetStore[F[_], G[_]: Functor, K, E](
      queries: PostgresEventJournalQueries[F, K, E],
      offsetStore: OffsetStore[G]) {

    private def wrap(tagConsumer: TagConsumer,
                     underlying: (
                         EventTag,
                         Offset) => Stream[F, (Offset, EntityEvent[K, E])])
      : G[Stream[F, Committable[G, (Offset, EntityEvent[K, E])]]] =
      offsetStore.getValue(tagConsumer).map { committedOffset =>
        val effectiveOffset = committedOffset.getOrElse(Offset.zero)
        underlying(tagConsumer.tag, effectiveOffset)
          .map {
            case x @ (offset, _) =>
              Committable(offsetStore.setValue(tagConsumer, offset), x)
          }
      }

    def eventsByTag(tag: EventTag, consumerId: ConsumerId)
      : G[Stream[F, Committable[G, (Offset, EntityEvent[K, E])]]] =
      wrap(TagConsumer(tag, consumerId), queries.eventsByTag)

    def currentEventsByTag(tag: EventTag, consumerId: ConsumerId)
      : G[Stream[F, Committable[G, (Offset, EntityEvent[K, E])]]] =
      wrap(TagConsumer(tag, consumerId), queries.currentEventsByTag)
  }
}

trait PostgresEventJournalQueries[F[_], K, E] {
  protected def sleepBeforePolling: F[Unit]

  final def eventsByTag(
      tag: EventTag,
      offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] = {
    currentEventsByTag(tag, offset).zipWithNext
      .flatMap {
        case (x, Some(_)) => Stream.emit(x)
        case (x @ (latestOffset, _), None) =>
          Stream
            .emit(x)
            .append(Stream
              .eval(sleepBeforePolling) >> eventsByTag(tag, latestOffset))

      }
      .append(Stream
        .eval(sleepBeforePolling) >> eventsByTag(tag, offset))

  }
  def currentEventsByTag(tag: EventTag,
                         offset: Offset): Stream[F, (Offset, EntityEvent[K, E])]

  final def withOffsetStore[G[_]: Functor](
      offsetStore: KeyValueStore[G, TagConsumer, Offset])
    : PostgresEventJournalQueries.WithOffsetStore[F, G, K, E] =
    new PostgresEventJournalQueries.WithOffsetStore(this, offsetStore)
}
