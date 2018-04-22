package aecor.journal.postgres

import java.util.UUID

import aecor.data.{ConsumerId, EventTag, TagConsumer}
import cats.effect.IO
import doobie.util.transactor.Transactor
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import doobie.implicits._

class PostgresOffsetStoreTest
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {
  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    s"jdbc:postgresql://localhost:5432/postgres",
    "notxcain",
    "1"
  )
  val store = PostgresOffsetStore(
    s"offset_test_${UUID.randomUUID().toString.replace('-', '_')}")

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

  override protected def beforeAll(): Unit = {
    store.createTable.transact(xa).unsafeRunSync()
  }

  override protected def afterAll(): Unit = {
    store.dropTable.transact(xa).unsafeRunSync()
  }
}
