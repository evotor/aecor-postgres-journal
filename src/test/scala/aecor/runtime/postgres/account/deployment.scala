package aecor.runtime.postgres.account

import aecor.data.{ EitherK, EventTag, Tagging }
import aecor.journal.postgres._
import aecor.runtime.Eventsourced
import aecor.runtime.Eventsourced.Snapshotting
import aecor.runtime.eventsourced.ActionRunner
import aecor.runtime.postgres.PostgresRuntime
import aecor.runtime.postgres.account.EventsourcedAlgebra.AccountState
import cats.Monad
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

object deployment {
  val tagging: Tagging[AccountId] = Tagging.partitioned[AccountId](160)(EventTag("Account"))

  val schema = JournalSchema("account_event")

  val journal =
    PostgresEventJournal(schema, tagging, AccountEvent.serializer)

  val snapshotStore: PostgresSnapshotStore[AccountId, AccountState] =
    PostgresSnapshotStore[AccountId, AccountState]("account_snapshot")

  val snapshotting: Snapshotting[ConnectionIO, AccountId, Option[AccountState]] =
    Snapshotting.snapshotEach(40L, new OptionalKeyValueStore(snapshotStore))

  def deploy[F[_]: Monad](xa: Transactor[F]): Accounts[F] = {

    val behaviorCIO = EventsourcedAlgebra.behavior[ConnectionIO]

    val behavior: AccountId => EitherK[Algebra, Rejection, ConnectionIO] = { key: AccountId =>
      ActionRunner(key, behaviorCIO.initial, behaviorCIO.update, journal)
        .modifyStrategy(_.withSnapshotting(snapshotting))
        .runActions(behaviorCIO.actions)
    }

    Eventsourced.Entities.fromEitherK(
      PostgresRuntime
        .runBehavior("Account", behavior, xa)
    )
  }
}
