package aecor.journal.postgres

import java.util.UUID

import aecor.data.{ConsumerId, EventTag, TagConsumer}
import cats.effect.IO
import doobie.implicits._
import doobie.util.transactor.Transactor
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers

class PostgresOffsetStoreTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  implicit val contextShift = IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer = IO.timer(scala.concurrent.ExecutionContext.global)
  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    s"jdbc:postgresql://localhost:5432/postgres",
    "user",
    ""
  )
  val store = PostgresOffsetStore(s"offset_test_${UUID.randomUUID().toString.replace('-', '_')}")

  val tagConsumer = TagConsumer(EventTag("TheTag"), ConsumerId("Cnzmrr"))

  test("Stores and retrieves offsets") {
    val offset1 = Offset(100500L)
    val offset2 = Offset(100501L)
    val program = for {
      _ <- store.setValue(tagConsumer, offset1).transact(xa)
      out1 <- store.getValue(tagConsumer).transact(xa)
      _ <- store.setValue(tagConsumer, offset2).transact(xa)
      out2 <- store.getValue(tagConsumer).transact(xa)
    } yield (out1, out2)

    assert(program.unsafeRunSync() == ((Some(offset1), Some(offset2))))
  }

  test("Stores and deletes offsets") {
    val offset1 = Offset(100500L)
    val program = for {
      _ <- store.setValue(tagConsumer, offset1).transact(xa)
      out1 <- store.getValue(tagConsumer).transact(xa)
      _ <- store.deleteValue(tagConsumer).transact(xa)
      out2 <- store.getValue(tagConsumer).transact(xa)
    } yield (out1, out2)

    assert(program.unsafeRunSync() == ((Some(offset1), None)))
  }

  override protected def beforeAll(): Unit =
    store.createTable.transact(xa).unsafeRunSync()

  override protected def afterAll(): Unit =
    store.dropTable.transact(xa).unsafeRunSync()
}
