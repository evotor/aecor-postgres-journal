package aecor.runtime.postgres

import aecor.runtime.postgres.account._
import aecor.runtime.postgres.account.deployment._
import cats.effect.{ContextShift, IO, Resource}
import cats.implicits._
import doobie._
import doobie.hikari._
import doobie.implicits._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}

class PostgresRuntimeTest
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {

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
      te)
    _ <- Resource.liftF(xa.configure(x => IO(x.setMaximumPoolSize(32))))
  } yield xa

  private val (xa, shutdownTransactor) =
    createTransactor.allocated.unsafeRunSync()

  test("foo") {
    val accounts = deploy(xa)
    val rounds = 2
    val count = 30000
    val size = 2
    val start = System.currentTimeMillis()
    val result = (1 to rounds).toVector.map { round =>
      (1 to count).toVector
        .parTraverse { idx =>
          val foo = accounts(AccountId(s"$round-$idx"))
          val one = foo.open(checkBalance = false) >>
            (1 to size).toVector.parTraverse_ { x =>
              foo
                .credit(TransactionId(s"$x"), Amount(1))
            } >> foo.getBalance.map(_.right.get.asBigDecimal)
          one
        }.unsafeRunSync().sum
    }.sum
    val end = System.currentTimeMillis()
    val rate = (count * (size + 1) * rounds / ((end - start) / 1000.0)).floor
    println(s"$rate btx/s")
    assert(result == BigDecimal(count * size * rounds))
  }

  override protected def beforeAll(): Unit = {
    (journal.createTable >> snapshotStore.createTable)
      .transact(xa)
      .unsafeRunSync()
  }

  override protected def afterAll(): Unit = {
    ((journal.dropTable >> snapshotStore.dropTable).transact(xa) >> shutdownTransactor)
      .unsafeRunSync()
  }

}
