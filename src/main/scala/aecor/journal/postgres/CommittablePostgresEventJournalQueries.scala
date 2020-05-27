package aecor.journal.postgres

import aecor.data._
import aecor.journal.postgres.CommittablePostgresEventJournalQueries.OffsetStore
import aecor.runtime.KeyValueStore
import cats.Functor
import fs2.Stream
import cats.implicits._

final class CommittablePostgresEventJournalQueries[F[_]: Functor, K, E](
  queries: PostgresEventJournalQueries[F, K, E],
  offsetStore: OffsetStore[F]
) {

  private def wrap(
    tagConsumer: TagConsumer,
    limit: Int,
    underlying: (EventTag, Offset, Int) => Stream[F, (Offset, EntityEvent[K, E])]
  ): Stream[F, Committable[F, (Offset, EntityEvent[K, E])]] =
    Stream.eval(offsetStore.getValue(tagConsumer).map(_.getOrElse(Offset.zero))).flatMap { initialOffset =>
      underlying(tagConsumer.tag, initialOffset, limit)
        .map {
          case x @ (offset, _) =>
            Committable(offsetStore.setValue(tagConsumer, offset), x)
        }
    }

  def eventsByTag(
    tag: EventTag,
    consumerId: ConsumerId,
    limit: Int = 1024
  ): Stream[F, Committable[F, (Offset, EntityEvent[K, E])]] =
    wrap(TagConsumer(tag, consumerId), limit, queries.eventsByTag)

  def currentEventsByTag(
    tag: EventTag,
    consumerId: ConsumerId,
    limit: Int = 1024
  ): Stream[F, Committable[F, (Offset, EntityEvent[K, E])]] =
    wrap(TagConsumer(tag, consumerId), limit, queries.currentEventsByTag)
}

object CommittablePostgresEventJournalQueries {
  type OffsetStore[F[_]] = KeyValueStore[F, TagConsumer, Offset]
}
