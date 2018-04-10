package aecor.journal.postgres

import aecor.data._
import aecor.runtime.KeyValueStore
import cats.Monad
import fs2.Stream
import cats.implicits._

private[postgres] trait OffsetStoreSupport {
  final class WithOffsetStore[F[_], K, E](
      self: PostgresEventJournal[F, K, E],
      offsetStore: KeyValueStore[F, TagConsumer, Long])(implicit F: Monad[F]) {

    private def wrap(
        tagConsumer: TagConsumer,
        underlying: (EventTag, Long) => Stream[F, (Long, EntityEvent[K, E])])
      : Stream[F, Committable[F, (Long, EntityEvent[K, E])]] =
      Stream.force {
        for {
          storedOffset <- offsetStore.getValue(tagConsumer)
          effectiveOffset = storedOffset.getOrElse(0L)
        } yield
          underlying(tagConsumer.tag, effectiveOffset).map {
            case x @ (offset, _) =>
              Committable(offsetStore.setValue(tagConsumer, offset), x)
          }
      }

    def eventsByTag(tag: EventTag, consumerId: ConsumerId)
      : Stream[F, Committable[F, (Long, EntityEvent[K, E])]] =
      wrap(TagConsumer(tag, consumerId), self.eventsByTag)

    def currentEventsByTag(tag: EventTag, consumerId: ConsumerId)
      : Stream[F, Committable[F, (Long, EntityEvent[K, E])]] =
      wrap(TagConsumer(tag, consumerId), self.currentEventsByTag)
  }
}
