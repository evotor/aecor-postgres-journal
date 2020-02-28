package aecor.journal.postgres

import java.util.UUID

import aecor.data._
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import cats.data.NonEmptyChain
import cats.effect.{Blocker, IO}
import cats.implicits._
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import doobie.implicits._
import fs2._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers

class PostgresEventJournalEventsByTagGapTest extends AnyFunSuite with Matchers with BeforeAndAfterAll {
  implicit val contextShift =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  implicit val timer = IO.timer(scala.concurrent.ExecutionContext.global)

  val stringSerializer: Serializer[String] = new Serializer[String] {
    override def serialize(a: String): (TypeHint, Array[Byte]) =
      ("", a.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    override def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, String] =
      Right(new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
  }

  private val createTransactor = for {
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

  val schema = JournalSchema[String, String](s"test_${UUID.randomUUID().toString.replace('-', '_')}", stringSerializer)
  val tagging = Tagging.partitioned[String](10)(EventTag("test-skip"))
  val journal = schema.journal(tagging).transactK(xa)
  val queries = schema.queries(xa)
  override protected def beforeAll(): Unit =
    schema.create.transact(xa).unsafeRunSync()

  test("Journal doesn't allow gap in eventsByTag during concurrent writes") {
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

    val appendAllEvents = fs2.Stream.emits(allEvents).covary[IO].parEvalMapUnordered(100)(appendEvents).compile.drain

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

    val x = for {
      fiber <- (IO.shift >> foldEvents).start
      _ <- appendAllEvents
      out <- fiber.join
    } yield out

    assert(x.unsafeRunSync().contains(false))
  }

  override protected def afterAll(): Unit =
    (schema.drop.transact(xa) >> shutdownTransactor).unsafeRunSync()

}
