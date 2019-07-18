package aecor.journal.postgres

import aecor.runtime.KeyValueStore
import cats.effect.{Concurrent, Resource, Timer}
import cats.implicits._
import fs2.concurrent.{Enqueue, Queue}

import scala.concurrent.duration.FiniteDuration

final class BatchingKeyValueStore[F[_], K, A] private (store: KeyValueStore[F, K, A], enqueue: Enqueue[F, (K, A)])
    extends KeyValueStore[F, K, A] {
  override def setValue(key: K, value: A): F[Unit] =
    enqueue.enqueue1(key -> value)

  override def getValue(key: K): F[Option[A]] =
    store.getValue(key)

  override def deleteValue(key: K): F[Unit] =
    store.deleteValue(key)
}

object BatchingKeyValueStore {
  def resource[F[_]: Concurrent: Timer, K, A](store: KeyValueStore[F, K, A],
                                              batchSize: Int,
                                              timeWindow: FiniteDuration): Resource[F, KeyValueStore[F, K, A]] =
    fs2.Stream
      .eval(Queue.unbounded[F, (K, A)])
      .flatMap { queue =>
        val batchProcess =
          queue.dequeue.groupWithin(batchSize, timeWindow).evalMap { chunk =>
            chunk
              .foldLeft(Map.empty[K, A]) {
                case (map, (k, a)) =>
                  map.updated(k, a)
              }
              .toList
              .traverse_ {
                case (k, a) =>
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
