package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{ KeyDecoder, KeyEncoder }
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.EventJournal
import cats.Monad
import cats.data.NonEmptyChain
import cats.effect.Timer
import doobie._
import doobie.implicits._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

trait PostgresEventJournal[F[_], K, E] extends EventJournal[F, K, E] {
  def createTable: F[Unit]
  def dropTable: F[Unit]
  def currentEventsByTag(tag: EventTag, offset: Offset): Stream[F, (Offset, EntityEvent[K, E])]
}

object PostgresEventJournal {

  trait Serializer[A] {
    def serialize(a: A): (TypeHint, Array[Byte])
    def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, A]
  }
  object Serializer {
    type TypeHint = String
  }

  def apply[K: KeyEncoder: KeyDecoder, E](
    tableName: String,
    tagging: Tagging[K],
    serializer: Serializer[E]
  ): PostgresEventJournal[ConnectionIO, K, E] =
    PostgresEventJournalCIO(tableName, tagging, serializer)

  implicit final class PostgresEventJournalQueriesSyntax[F[_], K, E](
    val self: PostgresEventJournal[F, K, E]
  ) extends AnyVal {
    def queries(
      pollingInterval: FiniteDuration
    )(implicit timer: Timer[F]): PostgresEventJournalQueries[F, K, E] =
      new PostgresEventJournalQueries[F, K, E] {

        override protected val sleepBeforePolling: F[Unit] =
          timer.sleep(pollingInterval)

        def currentEventsByTag(tag: EventTag,
                               offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] =
          self.currentEventsByTag(tag, offset)
      }
  }

  implicit final class PostgresEventJournalCIOTransactSyntax[K, E](
    val self: PostgresEventJournal[ConnectionIO, K, E]
  ) extends AnyVal {
    def transact[F[_]: Monad](xa: Transactor[F]): PostgresEventJournal[F, K, E] =
      new PostgresEventJournal[F, K, E] {
        override def createTable: F[Unit] = self.createTable.transact(xa)

        override def dropTable: F[Unit] = self.dropTable.transact(xa)

        override def currentEventsByTag(tag: EventTag,
                                        offset: Offset): Stream[F, (Offset, EntityEvent[K, E])] =
          self.currentEventsByTag(tag, offset).transact(xa)

        override def append(entityKey: K, sequenceNr: Long, events: NonEmptyChain[E]): F[Unit] =
          self.append(entityKey, sequenceNr, events).transact(xa)

        override def foldById[S](entityKey: K, sequenceNr: Long, initial: S)(
          f: (S, E) => Folded[S]
        ): F[Folded[S]] =
          self.foldById(entityKey, sequenceNr, initial)(f).transact(xa)
      }
  }
}
