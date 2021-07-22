package aecor.tests.journal.postgres

import java.util.UUID

import aecor.data._
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.postgres.{JournalSchema, Offset}
import aecor.tests.PostgresTest
import cats.data.NonEmptyChain
import cats.effect.{ContextShift, IO, Timer}
import cats.implicits._
import doobie.implicits._
import fs2._
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class PostgresEventJournalEventsByTagGapTest extends AsyncFlatSpec with PostgresTest[IO] with Matchers {
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
  private val tagging = Tagging.partitioned[String](10)(EventTag("test-skip"))

  "PostgresEventJournalEventsByTagGap" should "Journal doesn't allow gap in eventsByTag during concurrent writes" in effectTest {
    newDatabaseResource
      .use { xa =>
        val journal = schema.journal(tagging).transactK(xa)
        val queries = schema.queries(xa)

        val eventsForEachAggregate = 400L
        val aggregateIds = List(
          "q",
          "w",
          "e",
          "r",
          "t",
          "y",
          "u",
          "i",
          "o",
          "p",
          "a",
          "s",
          "d",
          "f",
          "g",
          "h",
          "j",
          "k",
          "l",
          "z"
        )

        def aggergateEvents(id: String) =
          (1L to eventsForEachAggregate).map(i => (i, id, s"$id$i")).toList

        val allEvents =
          aggregateIds.map(aggergateEvents)

        val allEventsCount = aggregateIds.size * eventsForEachAggregate

        def appendEvents(events: List[(Long, String, String)]) = events.grouped(10).toList.traverse {
          case (offset, id, content) :: others =>
            journal.append(id, offset, NonEmptyChain(content, others.map(_._3): _*))
          case Nil =>
            IO.unit
        }

        val appendAllEvents =
          fs2.Stream.emits(allEvents).covary[IO].parEvalMapUnordered(100)(appendEvents).compile.drain

        val foldEvents = Stream
          .emits(tagging.tags)
          .map { tag =>
            queries
              .eventsByTag(tag, Offset(0L))
          }
          .parJoinUnbounded
          .scan((false, Map.empty[String, Long])) {
            case (acc @ (hasHole, seqNrs), (_, EntityEvent(key, seqNr, _))) =>
              if (hasHole)
                acc
              else if (seqNrs.getOrElse(key, 0L) + 1 != seqNr) {
                (true, seqNrs)
              } else
                (false, seqNrs.updated(key, seqNr))
          }
          .takeWhile({
            case (hasHole, counters) =>
              !hasHole && counters.values.sum != allEventsCount
          }, true)
          .map(_._1)
          .compile
          .last

        for {
          _ <- schema.create.transact(xa)
          fiber <- (IO.shift >> foldEvents).start
          _ <- appendAllEvents
          out <- fiber.join
        } yield out
      }
      .map(_ shouldEqual Some(false))
  }
}
