package aecor.journal.postgres

import aecor.runtime.KeyValueStore
import cats.effect.std.Queue
import cats.effect.{Resource, Temporal}
import cats.syntax.all._

import scala.concurrent.duration.FiniteDuration

final class BatchingKeyValueStore[F[_], K, A] private (store: KeyValueStore[F, K, A], enqueue: Queue[F, Option[(K, A)]])
    extends KeyValueStore[F, K, A] {
  override def setValue(key: K, value: A): F[Unit] =
    enqueue.offer((key -> value).some)

  override def getValue(key: K): F[Option[A]] =
    store.getValue(key)

  override def deleteValue(key: K): F[Unit] =
    store.deleteValue(key)
}

object BatchingKeyValueStore {
  def resource[F[_]: Temporal, K, A](store: KeyValueStore[F, K, A],
                                     batchSize: Int,
                                     timeWindow: FiniteDuration
  ): Resource[F, KeyValueStore[F, K, A]] =
    fs2.Stream
      .eval(Queue.unbounded[F, Option[(K, A)]])
      .flatMap { queue =>
        val batchProcess =
          fs2.Stream
            .fromQueueNoneTerminated(queue)
            .groupWithin(batchSize, timeWindow)
            .evalMap { chunk =>
              chunk
                .foldLeft(Map.empty[K, A]) { case (map, (k, a)) =>
                  map.updated(k, a)
                }
                .toList
                .traverse_ { case (k, a) =>
                  store.setValue(k, a)
                }
            }

        fs2.Stream
          .emit(new BatchingKeyValueStore(store, queue))
          .covaryAll[F, KeyValueStore[F, K, A]]
          .concurrently(batchProcess)
      }
      .compile
      .resource
      .lastOrError
}
