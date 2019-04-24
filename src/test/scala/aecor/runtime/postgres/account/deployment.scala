package aecor.runtime.postgres.account

import aecor.data.{EventTag, Tagging}
import aecor.journal.postgres._
import aecor.runtime.Eventsourced
import aecor.runtime.Eventsourced.Snapshotting
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

  private val behaviorCIO = EventsourcedAlgebra.behavior[ConnectionIO]

  def deploy[F[_]: Monad](xa: Transactor[F]): Accounts[F] = {

    Snapshotting.snapshotEach(
      40L,
      new OptionalKeyValueStore(snapshotStore).mapK(xa.trans)
    )

    val behavior =
      PostgresRuntime("Account", behaviorCIO, journal, Snapshotting.noSnapshot[F, AccountId, Option[AccountState]], xa)

    Eventsourced.Entities.fromEitherK(
      behavior
    )
  }
}
