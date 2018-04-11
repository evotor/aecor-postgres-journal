package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{KeyDecoder, KeyEncoder}
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.postgres.internal.ConnectionIOPostgresEventJournal
import aecor.runtime.EventJournal
import cats.data.NonEmptyVector
import cats.effect.{IO, Timer}
import cats.~>
import doobie._
import fs2._

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
  final case class EntityName(value: String) extends AnyVal
  object EntityName {
    implicit val composite: Composite[EntityName] =
      Composite[String].imap(EntityName(_))(_.value)
  }

  final case class Settings(tableName: String, pollingInterval: FiniteDuration)

  def apply[F[_], K, E](transactor: Transactor[F],
                        settings: PostgresEventJournal.Settings,
                        entityName: EntityName,
                        tagging: Tagging[K],
                        serializer: Serializer[E])(
      implicit
      encodeKey: KeyEncoder[K],
      decodeKey: KeyDecoder[K],
      timer: Timer[F]): PostgresEventJournal[F, K, E] =
    new ConnectionIOPostgresEventJournal(transactor,
                                         settings,
                                         entityName,
                                         tagging,
                                         serializer)

}

abstract class PostgresEventJournal[F[_], K, E]
    extends EventJournal[F, K, E]
    with PostgresEventJournalQueries[F, K, E] {
  outer =>
  private[postgres] def dropTable: F[Unit]
  def createTable: F[Unit]

  def mapK[G[_]](f: F ~> G): PostgresEventJournal[G, K, E] =
    new PostgresEventJournal[G, K, E] {
      override private[postgres] def dropTable: G[Unit] = f(outer.dropTable)

      override def createTable: G[Unit] = f(outer.createTable)

      override def eventsByTag(
          tag: EventTag,
          offset: Long): Stream[G, (Long, EntityEvent[K, E])] =
        outer.eventsByTag(tag, offset).translate(f)

      override def currentEventsByTag(
          tag: EventTag,
          offset: Long): Stream[G, (Long, EntityEvent[K, E])] =
        outer.currentEventsByTag(tag, offset).translate(f)

      override def append(entityKey: K,
                          offset: Long,
                          events: NonEmptyVector[E]): G[Unit] =
        f(outer.append(entityKey, offset, events))

      override def foldById[S](entityKey: K, offset: Long, zero: S)(
          fold: (S, E) => Folded[S]): G[Folded[S]] =
        f(outer.foldById(entityKey, offset, zero)(fold))
    }
}
