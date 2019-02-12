package aecor.runtime.postgres.account

import aecor.data.EitherK
import aecor.journal.postgres.{
  OptionalKeyValueStore,
  PostgresEventJournalCIO,
  PostgresSnapshotStore
}
import aecor.runtime.Eventsourced.{ Snapshotting, Versioned }
import aecor.runtime.eventsourced.ActionRunner
import aecor.runtime.postgres.PostgresRuntime
import aecor.runtime.postgres.account.EventsourcedAlgebra.AccountState
import aecor.runtime.{ Eventsourced, KeyValueStore }
import cats.Monad
import doobie.free.connection.ConnectionIO
import doobie.implicits._
import doobie.util.transactor.Transactor

object deployment {
  val journal =
    PostgresEventJournalCIO("account_event", EventsourcedAlgebra.tagging, AccountEvent.serializer)

  val snapshotStore: PostgresSnapshotStore[AccountId, AccountState] =
    PostgresSnapshotStore[AccountId, AccountState]("account_snapshot")

  val ss = new KeyValueStore[ConnectionIO, AccountId, Versioned[AccountState]] {
    val map = scala.collection.concurrent.TrieMap.empty[AccountId, Versioned[AccountState]]
    override def setValue(key: AccountId, value: Versioned[AccountState]): ConnectionIO[Unit] =
      AsyncConnectionIO.delay(map.update(key, value))

    override def getValue(key: AccountId): ConnectionIO[Option[Versioned[AccountState]]] =
      AsyncConnectionIO.delay(map.get(key))

    override def deleteValue(key: AccountId): ConnectionIO[Unit] =
      AsyncConnectionIO.delay(map -= key)
  }

  val snapshotting: Snapshotting[ConnectionIO, AccountId, Option[AccountState]] =
    Snapshotting.snapshotEach(1L, new OptionalKeyValueStore(ss))
  snapshotting.hashCode()

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
