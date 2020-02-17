package aecor.runtime.postgres

import aecor.runtime.postgres.account._
import aecor.runtime.postgres.account.deployment._
import cats.effect._
import cats.implicits._
import doobie._
import doobie.hikari._
import doobie.implicits._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PostgresRuntimeTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {

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
      Blocker.liftExecutionContext(te)
    )
  } yield xa

  private val (xa, shutdownTransactor) =
    createTransactor.allocated.unsafeRunSync()

  test("foo") {
    val accounts = deploy(xa)
    val rounds = 10000
    val parallelism = 30
    val size = 2
    val amount = 1
    val transactions = (1 to size).toVector.map(x => TransactionId(s"$x"))

    val program = fs2.Stream
      .range(0, rounds)
      .map(x => accounts(AccountId(s"$x")))
      .covary[IO]
      .mapAsyncUnordered(parallelism) { account =>
        val open = account.open(checkBalance = false)
        val credit = transactions.parTraverse_(account.credit(_, amount))
        open >> credit
      }
      .compile
      .drain
      .as(rounds * (size + 1))

    val start = System.currentTimeMillis()
    val total = program.unsafeRunSync()
    val end = System.currentTimeMillis()
    val rate = (total / ((end - start) / 1000.0)).floor
    println(s"$rate btx/s")
    assert(true)
//    assert(result == BigDecimal(count * size * rounds))
  }

  override protected def beforeAll(): Unit =
    (schema.create >> snapshotStore.createTable)
      .transact(xa)
      .unsafeRunSync()

  override protected def afterAll(): Unit =
    ((schema.drop >> snapshotStore.dropTable).transact(xa) >> shutdownTransactor)
      .unsafeRunSync()

}
