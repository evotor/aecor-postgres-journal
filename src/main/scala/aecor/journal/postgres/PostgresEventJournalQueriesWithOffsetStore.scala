package aecor.journal.postgres

import aecor.data._
import aecor.journal.postgres.PostgresEventJournalQueriesWithOffsetStore.OffsetStore
import aecor.runtime.KeyValueStore
import cats.Functor
import fs2.Stream
import cats.implicits._

final class PostgresEventJournalQueriesWithOffsetStore[F[_], G[_]: Functor, K, E](
  queries: PostgresEventJournalQueries[F, K, E],
  offsetStore: OffsetStore[G]
) {

  private def wrap(
    tagConsumer: TagConsumer,
    underlying: (EventTag, Offset) => Stream[F, (Offset, EntityEvent[K, E])]
  ): G[Stream[F, Committable[G, (Offset, EntityEvent[K, E])]]] =
    offsetStore.getValue(tagConsumer).map(_.getOrElse(Offset.zero)).map { initialOffset =>
      underlying(tagConsumer.tag, initialOffset)
        .map {
          case x @ (offset, _) =>
            Committable(offsetStore.setValue(tagConsumer, offset), x)
        }
    }

  def eventsByTag(
    tag: EventTag,
    consumerId: ConsumerId
  ): G[Stream[F, Committable[G, (Offset, EntityEvent[K, E])]]] =
    wrap(TagConsumer(tag, consumerId), queries.eventsByTag)

  def currentEventsByTag(
    tag: EventTag,
    consumerId: ConsumerId
  ): G[Stream[F, Committable[G, (Offset, EntityEvent[K, E])]]] =
    wrap(TagConsumer(tag, consumerId), queries.currentEventsByTag)
}

object PostgresEventJournalQueriesWithOffsetStore {
  type OffsetStore[F[_]] = KeyValueStore[F, TagConsumer, Offset]
}
