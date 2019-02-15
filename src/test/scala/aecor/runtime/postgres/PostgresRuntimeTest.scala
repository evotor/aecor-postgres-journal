package aecor.runtime.postgres

import aecor.runtime.postgres.account._
import aecor.runtime.postgres.account.deployment._
import cats.effect._
import cats.implicits._
import doobie._
import doobie.hikari._
import doobie.implicits._
import org.scalatest.{ BeforeAndAfterAll, FunSuite, Matchers }
import fs2.Stream
import scala.io.StdIn

object Run extends IOApp {

  private val createTransactor = for {
    ce <- ExecutionContexts
           .cachedThreadPool[IO] //ExecutionContexts.fixedThreadPool[IO](512) // our connect EC
    te <- ExecutionContexts.cachedThreadPool[IO]
//    ce = te
    xa <- HikariTransactor.newHikariTransactor[IO](
           "org.postgresql.Driver",
           "jdbc:postgresql://test-pgpool03.market.local/aecor_runtime_test",
           "monkey_user",
           "monkey_password",
           ce,
           te
         )
    _ = HikariTransactor.newHikariTransactor[IO](
      "org.postgresql.Driver",
      "jdbc:postgresql://localhost/postgres",
      "user",
      "",
      ce,
      te
    )
    _ <- Resource.liftF(xa.configure { x =>
          IO {
            x.setMaximumPoolSize(256)
            x.setAutoCommit(false)
          }
        })
  } yield xa

  override def run(args: List[String]): IO[ExitCode] = {
    val rounds = 50
    val parallelism = 256
    val size = 1200
    val amount = Amount(1)
    val transactions = (1 to size).toVector.map(x => TransactionId(s"$x"))
    createTransactor.use { xa =>
      for {
        _ <- IO {
              print("Press enter to continue >>")
              StdIn.readLine
            }
        _ <- (journal.createTable >> snapshotStore.createTable).transact(xa)
        accounts = deploy(xa)
        program = fs2.Stream
          .range(0, rounds)
//          .range(rounds, rounds + rounds)
          .map(x => accounts(AccountId(s"$x")))
          .covary[IO]
          .map { account =>
            val open = Stream.eval(account.open(checkBalance = false))
            val credit = Stream(transactions: _*)
              .covary[IO]
              .mapAsyncUnordered(5)(account.credit(_, amount))
            open >> credit
          }
          .parJoin(parallelism)
          .compile
          .drain
          .as(rounds * (size + 1))

        start <- IO(System.currentTimeMillis())
        total <- program
        end <- IO(System.currentTimeMillis())
        _ = println(total)
        rate = (total / ((end - start) / 1000.0)).floor
        _ <- IO(println(s"$rate btx/s"))
        _ <- (journal.dropTable >> snapshotStore.dropTable).transact(xa)
      } yield ExitCode.Success
    }
  }
}

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
    val rounds = 10000
    val parallelism = 30
    val size = 2
    val amount = Amount(1)
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
    (journal.createTable >> snapshotStore.createTable)
      .transact(xa)
      .unsafeRunSync()

  override protected def afterAll(): Unit =
    ((journal.dropTable >> snapshotStore.dropTable).transact(xa) >> shutdownTransactor)
      .unsafeRunSync()

}
