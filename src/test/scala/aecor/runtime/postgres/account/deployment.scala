package aecor.runtime.postgres.account

import aecor.data.EitherK
import aecor.journal.postgres.{PostgresEventJournalCIO, PostgresSnapshotStore}
import aecor.runtime.Eventsourced
import aecor.runtime.Eventsourced.Snapshotting
import aecor.runtime.eventsourced.StateStrategy
import aecor.runtime.postgres.PostgresRuntime
import aecor.runtime.postgres.account.EventsourcedAlgebra.AccountState
import cats.Monad
import doobie._
import doobie.implicits._
import doobie.util.transactor.Transactor

object deployment {
  val journal = PostgresEventJournalCIO("account_event",
                                        EventsourcedAlgebra.tagging,
                                        AccountEvent.serializer)

  val snapshotStore: PostgresSnapshotStore[AccountId, AccountState] =
    PostgresSnapshotStore[AccountId, AccountState]("account_snapshot")
  val snapshotting: Snapshotting[ConnectionIO, AccountId, Option[AccountState]] =
    Snapshotting.snapshotEach(1L, snapshotStore.optional)
  snapshotting.hashCode()

  def deploy[F[_]: Monad](xa: Transactor[F]): Accounts[F] = {

    val behaviorCIO = EventsourcedAlgebra.behavior[ConnectionIO]

    val behavior: AccountId => EitherK[Algebra, Rejection, ConnectionIO] = {
      key: AccountId =>
        StateStrategy
          .basic(key, behaviorCIO.initial, behaviorCIO.update, journal)
          .runActions(behaviorCIO.actions)
    }

    Eventsourced.Entities.fromEitherK(
      PostgresRuntime
        .runBehavior("Account", behavior, xa))
  }
}
