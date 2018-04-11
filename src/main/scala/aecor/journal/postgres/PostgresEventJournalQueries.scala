package aecor.journal.postgres

import aecor.data._
import aecor.runtime.KeyValueStore
import cats.Functor
import cats.implicits._
import fs2.Stream

object PostgresEventJournalQueries {
  final class WithOffsetStore[F[_], G[_]: Functor, K, E](
      queries: PostgresEventJournalQueries[F, K, E],
      offsetStore: KeyValueStore[G, TagConsumer, Long]) {

    private def wrap(
        tagConsumer: TagConsumer,
        underlying: (EventTag, Long) => Stream[F, (Long, EntityEvent[K, E])])
      : G[Stream[F, Committable[G, (Long, EntityEvent[K, E])]]] =
      offsetStore.getValue(tagConsumer).map { storedOffset =>
        underlying(tagConsumer.tag, storedOffset.getOrElse(0L)).map {
          case x @ (offset, _) =>
            Committable(offsetStore.setValue(tagConsumer, offset), x)
        }
      }

    def eventsByTag(tag: EventTag, consumerId: ConsumerId)
      : G[Stream[F, Committable[G, (Long, EntityEvent[K, E])]]] =
      wrap(TagConsumer(tag, consumerId), queries.eventsByTag)

    def currentEventsByTag(tag: EventTag, consumerId: ConsumerId)
      : G[Stream[F, Committable[G, (Long, EntityEvent[K, E])]]] =
      wrap(TagConsumer(tag, consumerId), queries.currentEventsByTag)
  }
}

trait PostgresEventJournalQueries[F[_], K, E] {
  def eventsByTag(tag: EventTag,
                  offset: Long): Stream[F, (Long, EntityEvent[K, E])]
  def currentEventsByTag(tag: EventTag,
                         offset: Long): Stream[F, (Long, EntityEvent[K, E])]

  final def withOffsetStore[G[_]: Functor](
      offsetStore: KeyValueStore[G, TagConsumer, Long])
    : PostgresEventJournalQueries.WithOffsetStore[F, G, K, E] =
    new PostgresEventJournalQueries.WithOffsetStore(this, offsetStore)
}
