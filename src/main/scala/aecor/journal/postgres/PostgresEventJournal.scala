package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.EventJournal
import cats.Monad
import cats.data.NonEmptyChain
import cats.effect.Timer
import doobie._
import doobie.implicits._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

object PostgresEventJournal {
  trait Serializer[A] {
    def serialize(a: A): (TypeHint, Array[Byte])
    def deserialize(typeHint: TypeHint,
                    bytes: Array[Byte]): Either[Throwable, A]
  }
  object Serializer {
    type TypeHint = String
  }

  def apply[F[_]: Monad, K: KeyEncoder: KeyDecoder, E](
      transactor: Transactor[F],
      tableName: String,
      tagging: Tagging[K],
      serializer: Serializer[E]): PostgresEventJournal[F, K, E] =
    new PostgresEventJournal(
      PostgresEventJournalCIO(tableName, tagging, serializer),
      transactor)

}

final class PostgresEventJournal[F[_], K, E](
    cio: PostgresEventJournalCIO[K, E],
    xa: Transactor[F])(implicit F: Monad[F])
    extends EventJournal[F, K, E] {

  def createTable: F[Unit] = cio.createTable.transact(xa)

  private[postgres] def dropTable: F[Unit] = cio.dropTable.transact(xa)

  override def append(entityKey: K,
                      offset: Long,
                      events: NonEmptyChain[E]): F[Unit] =
    cio.append(entityKey, offset, events).transact(xa)

  override def foldById[S](key: K, offset: Long, zero: S)(
      f: (S, E) => Folded[S]): F[Folded[S]] =
    cio.foldById(key, offset, zero)(f).transact(xa)

  def queries(pollingInterval: FiniteDuration)(
      implicit timer: Timer[F]): PostgresEventJournalQueries[F, K, E] =
    new PostgresEventJournalQueries[F, K, E] {

      override protected val sleepBeforePolling: F[Unit] =
        timer.sleep(pollingInterval)

      /**
        * Streams all existing events tagged with tag, starting from offset exclusive
        * @param tag - tag to be used as a filter
        * @param offset - offset to start from, exclusive
        * @return - a stream of events which terminates when reaches the last existing event.
        */
      def currentEventsByTag(
          tag: EventTag,
          offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] =
        cio.currentEventsByTag(tag, offset).transact(xa)
    }

}
