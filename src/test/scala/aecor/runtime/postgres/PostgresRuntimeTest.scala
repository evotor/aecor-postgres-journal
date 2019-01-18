package aecor.runtime.postgres

import aecor.runtime.postgres.account._
import cats.effect.{ContextShift, IO}
import doobie.util.transactor.Transactor
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import doobie._
import doobie.implicits._
import cats.implicits._
import deployment._
import cats.effect.Resource
import doobie.hikari._

class PostgresRuntimeTest
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {

  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  private val oxa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    s"jdbc:postgresql://localhost:5432/postgres",
    "user",
    ""
  )

  oxa.hashCode()

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

  private val (xa, shutdown) = createTransactor.allocated.unsafeRunSync()

  test("foo") {
    val accounts = deploy(xa)
    val count = 60000
    val size = 2
    val program = (1 to count).toVector
      .map { idx =>
        val foo = accounts(AccountId(s"foo$idx"))
        val one = foo.open(checkBalance = false) >>
          (1 to size).toVector.traverse_ { x =>
            foo
              .credit(TransactionId(s"$x"), Amount(1))
          } >> foo.getBalance.map(_.right.get.asBigDecimal)
        one
      }
      .parTraverse(_.start)
      .flatMap(_.parTraverse(_.join))
    assert(program.unsafeRunSync().sum == BigDecimal(count * size))
  }

  override protected def beforeAll(): Unit = {
    (journal.createTable >> snapshotStore.createTable)
      .transact(xa)
      .unsafeRunSync()
  }

  override protected def afterAll(): Unit = {
    ((journal.dropTable >> snapshotStore.dropTable).transact(xa) >> shutdown)
      .unsafeRunSync()
  }

}
