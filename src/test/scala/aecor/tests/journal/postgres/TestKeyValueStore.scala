package aecor.tests.journal.postgres

import aecor.runtime.KeyValueStore
import cats.effect.Sync

object TestKeyValueStore {
  final class Builder[F[_]] {
    def apply[K, V](state: Map[K, V])(implicit F: Sync[F]): F[KeyValueStore[F, K, V]] =
      F.delay(new TestKeyValueStore(scala.collection.concurrent.TrieMap(state.toVector: _*)))
  }

  def apply[F[_]]: Builder[F] = new Builder[F]
  def empty[F[_], K, V](implicit F: Sync[F]): F[KeyValueStore[F, K, V]] =
    TestKeyValueStore[F](Map.empty[K, V])
}

final class TestKeyValueStore[F[_], K, V](store: scala.collection.concurrent.TrieMap[K, V])(implicit
    F: Sync[F]
) extends KeyValueStore[F, K, V] {
  override def setValue(key: K, value: V): F[Unit] =
    F.delay(store.update(key, value))

  override def getValue(key: K): F[Option[V]] =
    F.delay(store.get(key))

  override def deleteValue(key: K): F[Unit] = F.delay(store -= key)
}
