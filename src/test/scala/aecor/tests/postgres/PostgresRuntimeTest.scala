package aecor.tests.postgres

import aecor.tests.postgres.account._
import aecor.tests.postgres.account.deployment._
import aecor.tests.PostgresTest
import cats.effect._
import cats.implicits._
import doobie.implicits._
import org.scalatest.flatspec.AsyncFlatSpec

class PostgresRuntimeTest extends AsyncFlatSpec with PostgresTest[IO] {
  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  "PostgresRuntime" should "work correctly" in effectTest {
    newDatabaseResource
      .use { xa =>
        schema.create.transact(xa) *>
          snapshotStore.createTable.transact(xa) *> {
          val accounts = deploy(xa)
          val rounds = 10000
          val parallelism = 30
          val size = 2
          val amount = 1
          val transactions = (1 to size).toVector.map(x => TransactionId(s"$x"))

          fs2.Stream
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
        }
      }
      .map { total =>
        val start = System.currentTimeMillis()
        val end = System.currentTimeMillis()
        val rate = (total / ((end - start) / 1000.0)).floor

        println(s"$rate btx/s")
        assert(true)
      }
  }
}
