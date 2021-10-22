package aecor.tests.journal.postgres

import java.util.UUID

import aecor.data._
import aecor.journal.postgres.{ JournalSchema, Offset }
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.tests.PostgresTest
import cats.data.NonEmptyChain
import cats.effect.{ ContextShift, IO, Timer }
import doobie.implicits._
import org.postgresql.util.PSQLException
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class PostgresEventJournalTest extends AsyncFlatSpec with PostgresTest[IO] with Matchers {
  implicit val contextShift: ContextShift[IO] =
    IO.contextShift(scala.concurrent.ExecutionContext.global)
  implicit val timer: Timer[IO] = IO.timer(scala.concurrent.ExecutionContext.global)

  private val stringSerializer: Serializer[String] = new Serializer[String] {
    override def serialize(a: String): (TypeHint, Array[Byte]) =
      ("", a.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    override def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, String] =
      Right(new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
  }

  private val schema =
    JournalSchema[String, String](s"test_${UUID.randomUUID().toString.replace('-', '_')}", stringSerializer)
  private val tagging = Tagging.const[String](EventTag("test"))
  private val consumerId = ConsumerId("C1")

  "PostgresEventJournal" should "append and fold events from zero offset" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)

        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          folded <- journal.read("a", 1L).map(_.payload).compile.toVector
        } yield folded
      }
      .map(_ shouldEqual Vector("1", "2"))
  }

  it should "append and fold events from non-zero offset" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)

        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          _ <- journal.append("a", 3L, NonEmptyChain("3"))
          folded <- journal.read("a", 2L).map(_.payload).compile.toVector
        } yield folded
      }
      .map(_ shouldEqual Vector("2", "3"))
  }

  it should "reject append at existing offset" in effectTest {
    newDatabaseResource.use { xa =>
      val journal = schema.journal(tagging).transactK(xa)
      val result =
        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          _ <- journal.append("a", 3L, NonEmptyChain("3"))
          _ <- journal.append("a", 3L, NonEmptyChain("4"))
        } yield ()

      result
        .handleErrorWith {
          case _: PSQLException => IO.unit
          case _                => IO.raiseError[Unit](new Throwable("Unexpected error type found"))
        }
        .as(succeed)
    }
  }

  it should "emit current events by tag" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)
        val journalQueries = schema.queries(xa, 1.second)

        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          _ <- journal.append("a", 3L, NonEmptyChain("3"))
          _ <- journal.append("b", 1L, NonEmptyChain("b1"))
          _ <- journal.append("a", 4L, NonEmptyChain("a4"))
          folded <- journalQueries
                      .currentEventsByTag(tagging.tag, Offset.zero)
                      .compile
                      .toVector
        } yield folded
      }
      .map(
        _ shouldEqual
          Vector(
            (Offset(1L), EntityEvent("a", 1, "1")),
            (Offset(2L), EntityEvent("a", 2, "2")),
            (Offset(3L), EntityEvent("a", 3, "3")),
            (Offset(4L), EntityEvent("b", 1, "b1")),
            (Offset(5L), EntityEvent("a", 4, "a4"))
          )
      )
  }

  it should "emit current events by tag from non zero offset" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)
        val journalQueries = schema.queries(xa, 1.second)

        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          _ <- journal.append("a", 3L, NonEmptyChain("3"))
          _ <- journal.append("b", 1L, NonEmptyChain("b1"))
          _ <- journal.append("a", 4L, NonEmptyChain("a4"))
          r <- journalQueries
                 .currentEventsByTag(tagging.tag, Offset(2L))
                 .compile
                 .toVector
        } yield r
      }
      .map(
        _ shouldEqual
          Vector(
            (Offset(3L), EntityEvent("a", 3, "3")),
            (Offset(4L), EntityEvent("b", 1, "b1")),
            (Offset(5L), EntityEvent("a", 4, "a4"))
          )
      )
  }

  it should "continuously emits events by tag" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)
        val journalQueries = schema.queries(xa, 1.second)

        val appendEvent =
          journal.append("b", 2L, NonEmptyChain("b2"))

        val foldEvents = journalQueries
          .eventsByTag(tagging.tag, Offset.zero)
          .take(6)
          .compile
          .toVector

        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          _ <- journal.append("a", 3L, NonEmptyChain("3"))
          _ <- journal.append("b", 1L, NonEmptyChain("b1"))
          _ <- journal.append("a", 4L, NonEmptyChain("a4"))
          fiber <- foldEvents.start
          _ <- appendEvent
          out <- fiber.join
        } yield out
      }
      .map(
        _ shouldEqual
          Vector(
            (Offset(1L), EntityEvent("a", 1L, "1")),
            (Offset(2L), EntityEvent("a", 2L, "2")),
            (Offset(3L), EntityEvent("a", 3L, "3")),
            (Offset(4L), EntityEvent("b", 1L, "b1")),
            (Offset(5L), EntityEvent("a", 4L, "a4")),
            (Offset(6L), EntityEvent("b", 2L, "b2"))
          )
      )
  }

  it should "continuously emits events by tag from non zero offset inclusive" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)
        val journalQueries = schema.queries(xa, 1.second)

        val appendEvent =
          journal.append("a", 5L, NonEmptyChain("a5"))

        val foldEvents = journalQueries
          .eventsByTag(tagging.tag, Offset(5L))
          .take(2)
          .compile
          .toVector

        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          _ <- journal.append("a", 3L, NonEmptyChain("3"))
          _ <- journal.append("b", 1L, NonEmptyChain("b1"))
          _ <- journal.append("a", 4L, NonEmptyChain("a4"))
          _ <- journal.append("b", 2L, NonEmptyChain("b2"))
          fiber <- foldEvents.start
          _ <- appendEvent
          out <- fiber.join
        } yield out
      }
      .map(
        _ shouldEqual Vector((Offset(6L), EntityEvent("b", 2L, "b2")), (Offset(7L), EntityEvent("a", 5L, "a5")))
      )
  }

  it should "correctly use offset store for current events by tag" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)
        val journalQueries = schema.queries(xa, 1.second)

        for {
          _ <- schema.create.transact(xa)
          _ <- journal.append("a", 1L, NonEmptyChain("1", "2"))
          _ <- journal.append("a", 3L, NonEmptyChain("3"))
          _ <- journal.append("b", 1L, NonEmptyChain("b1"))
          _ <- journal.append("a", 4L, NonEmptyChain("a4"))
          _ <- journal.append("b", 2L, NonEmptyChain("b2"))
          offset <- journalQueries
                      .currentEventsByTag(tagging.tag, Offset.zero)
                      .take(3)
                      .map(_._1)
                      .compile
                      .last
                      .map(_.getOrElse(Offset.zero))
          os <- TestKeyValueStore[IO](Map(TagConsumer(tagging.tag, consumerId) -> offset))
          runOnce = journalQueries
                      .withOffsetStore(os)
                      .currentEventsByTag(tagging.tag, consumerId)
                      .evalMap(_.commit)
                      .as(1)
                      .compile
                      .fold(0)(_ + _)
          processed1 <- runOnce
          _ <- journal.append("a", 6L, NonEmptyChain("a6"))
          processed2 <- runOnce
        } yield (processed1, processed2)
      }
      .map(_ shouldEqual 3 -> 1)
  }
}
