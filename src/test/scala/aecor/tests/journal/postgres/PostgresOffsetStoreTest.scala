package aecor.tests.journal.postgres

import java.util.UUID

import aecor.data.{ ConsumerId, EventTag, TagConsumer }
import aecor.journal.postgres.{ Offset, PostgresOffsetStore }
import aecor.tests.PostgresTest
import cats.effect.{ ContextShift, IO, Timer }
import doobie.implicits._
import org.scalatest.flatspec.AsyncFlatSpec

class PostgresOffsetStoreTest extends AsyncFlatSpec with PostgresTest[IO] {
  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(scala.concurrent.ExecutionContext.global)

  private val store = PostgresOffsetStore(
    s"offset_test_${UUID.randomUUID().toString.replace('-', '_')}"
  )

  private val tagConsumer = TagConsumer(EventTag("TheTag"), ConsumerId("Cnzmrr"))

  "PostgresOffsetStore" should "store and retrieve offsets" in effectTest {
    val offset1 = Offset(100500L)
    val offset2 = Offset(100501L)

    newDatabaseResource
      .use { xa =>
        for {
          _ <- store.createTable.transact(xa)
          _ <- store.setValue(tagConsumer, offset1).transact(xa)
          out1 <- store.getValue(tagConsumer).transact(xa)
          _ <- store.setValue(tagConsumer, offset2).transact(xa)
          out2 <- store.getValue(tagConsumer).transact(xa)
        } yield (out1, out2)
      }
      .map(result => assert(result == Some(offset1) -> Some(offset2)))
  }

  it should "store and delete offsets" in effectTest {
    val offset1 = Offset(100500L)

    newDatabaseResource
      .use { xa =>
        for {
          _ <- store.createTable.transact(xa)
          _ <- store.setValue(tagConsumer, offset1).transact(xa)
          out1 <- store.getValue(tagConsumer).transact(xa)
          _ <- store.deleteValue(tagConsumer).transact(xa)
          out2 <- store.getValue(tagConsumer).transact(xa)
        } yield (out1, out2)
      }
      .map(result => assert(result == Some(offset1) -> None))
  }
}
