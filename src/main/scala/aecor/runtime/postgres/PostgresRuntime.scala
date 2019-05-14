package aecor.runtime.postgres

import aecor.data.EventsourcedBehavior
import aecor.runtime.EventJournal
import aecor.runtime.Eventsourced.{Entities, Snapshotting}
import aecor.runtime.eventsourced.{ActionRunner, EventsourcedState}
import cats.effect.Bracket
import cats.implicits._
import cats.kernel.Hash
import cats.tagless.FunctorK
import cats.~>
import doobie._
import doobie.implicits._
import cats.tagless.implicits._

object PostgresRuntime {
  def wrapBefore(before: ConnectionIO[Unit]): ConnectionIO ~> ConnectionIO =
    Lambda[ConnectionIO ~> ConnectionIO] { ca =>
      before *> ca
    }

  def lockKey[K: Hash](typeName: String, key: K): doobie.ConnectionIO[Unit] =
    sql"""SELECT pg_advisory_xact_lock(${typeName.hashCode}, ${key.hash})"""
      .query[Unit]
      .unique

  def apply[M[_[_]]: FunctorK, F[_]: Bracket[?[_], Throwable], S, E, K: Hash](
    typeName: String,
    behavior: EventsourcedBehavior[M, ConnectionIO, S, E],
    journal: EventJournal[ConnectionIO, K, E],
    snapshotting: Snapshotting[F, K, S],
    transactor: Transactor[F]
  ): Entities[K, M, F] = {
    val strategy = EventsourcedState(behavior.initial, behavior.update, journal)
    Entities { key =>
      val runner = ActionRunner(
        key,
        strategy,
        snapshotting,
        wrapBefore(lockKey(typeName, key)).andThen(transactor.trans)
      )
      behavior.actions.mapK(runner)
    }
  }

}
