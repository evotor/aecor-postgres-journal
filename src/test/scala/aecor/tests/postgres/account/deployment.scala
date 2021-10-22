package aecor.tests.postgres.account

import aecor.data.{ EventTag, Tagging }
import aecor.journal.postgres._
import aecor.runtime.postgres.PostgresRuntime
import aecor.runtime.{ Eventsourced, Snapshotting }
import aecor.tests.postgres.account.EventsourcedAlgebra.AccountState
import cats.effect.Bracket
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

object deployment {
  val tagging: Tagging[AccountId] = Tagging.partitioned[AccountId](160)(EventTag("Account"))

  val schema = JournalSchema[AccountId, AccountEvent]("account_event", AccountEvent.serializer)

  val journal = schema.journal(tagging)

  val snapshotStore: PostgresSnapshotStore[AccountId, AccountState] =
    PostgresSnapshotStore[AccountId, AccountState]("account_snapshot")

  private val behaviorCIO = EventsourcedAlgebra.behavior[ConnectionIO]

  def deploy[F[_]: Bracket[*[_], Throwable]](xa: Transactor[F]): Accounts[F] = {

    Snapshotting.eachVersion(
      40L,
      new OptionalKeyValueStore(snapshotStore).mapK(xa.trans)
    )

    val behavior =
      PostgresRuntime("Account", behaviorCIO, journal, Snapshotting.disabled[F, AccountId, Option[AccountState]], xa)

    Eventsourced.Entities.rejectable(
      behavior
    )
  }
}
