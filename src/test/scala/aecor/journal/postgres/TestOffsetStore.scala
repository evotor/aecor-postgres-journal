package aecor.journal.postgres

import aecor.data.TagConsumer
import aecor.runtime.KeyValueStore
import cats.effect.IO

object TestOffsetStore {
  def apply(state: Map[TagConsumer, Long]): IO[TestOffsetStore] =
    IO(
      new TestOffsetStore(
        scala.collection.concurrent.TrieMap(state.toVector: _*)))

}

class TestOffsetStore(
    store: scala.collection.concurrent.TrieMap[TagConsumer, Long])
    extends KeyValueStore[IO, TagConsumer, Long] {
  override def setValue(key: TagConsumer, value: Long): IO[Unit] =
    IO(store.update(key, value))

  override def getValue(key: TagConsumer): IO[Option[Long]] =
    IO(store.get(key))
}
