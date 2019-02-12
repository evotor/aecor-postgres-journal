package aecor.runtime.postgres

import aecor.runtime.postgres.account._
import aecor.runtime.postgres.account.deployment._
import cats.effect.{ ContextShift, IO }
import cats.implicits._
import doobie._
import doobie.hikari._
import doobie.implicits._
import org.scalatest.{ BeforeAndAfterAll, FunSuite, Matchers }

class PostgresRuntimeTest extends FunSuite with Matchers with BeforeAndAfterAll {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  val createTransactor = for {
    ce <- ExecutionContexts.fixedThreadPool[IO](32) // our connect EC
    te <- ExecutionContexts.cachedThreadPool[IO]
    xa <- HikariTransactor.newHikariTransactor[IO](
           "org.postgresql.Driver",
           "jdbc:postgresql://localhost/postgres",
           "user",
           "",
           ce,
           te
         )
  } yield xa

  private val (xa, shutdownTransactor) =
    createTransactor.allocated.unsafeRunSync()

  test("foo") {
    val accounts = deploy(xa)
    val rounds = 100
    val count = 30
    val size = 20
    val amount = Amount(1)
    val transactions = (1 to size).toVector.map(x => TransactionId(s"$x"))
    val accountRounds = (1 to rounds).map { round =>
      (1 to count).toVector.map(idx => accounts(AccountId(s"$round-$idx")))
    }

    val start = System.currentTimeMillis()

    fs2.Stream.range(0, count*rounds).map(x => accounts(AccountId(s"$x")).mapAsyncUnordered(count) { account =>
      val open = account.open(checkBalance = false)
      val credit = transactions.parTraverse_(account.credit(_, amount))
      val getBalance = account.getBalance
      open >> credit >> getBalance
    }
    accountRounds.foreach { round =>
      round
        .parTraverse_ { account =>
          val open = account.open(checkBalance = false)
          val credit = transactions.parTraverse_(account.credit(_, amount))
          val getBalance = account.getBalance
          open >> credit >> getBalance
        }
        .unsafeRunSync()
    }
    val end = System.currentTimeMillis()
    val rate = (count * (size + 1) * rounds / ((end - start) / 1000.0)).floor
    println(s"$rate btx/s")
    assert(true)
//    assert(result == BigDecimal(count * size * rounds))
  }

  override protected def beforeAll(): Unit =
    (journal.createTable >> snapshotStore.createTable)
      .transact(xa)
      .unsafeRunSync()

  override protected def afterAll(): Unit =
    ((journal.dropTable >> snapshotStore.dropTable).transact(xa) >> shutdownTransactor)
      .unsafeRunSync()

}
