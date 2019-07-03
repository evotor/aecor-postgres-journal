package aecor.journal.postgres

import java.util.UUID

import aecor.data._
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import cats.data.NonEmptyChain
import cats.effect.IO
import cats.implicits._
import doobie.ExecutionContexts
import doobie.hikari.HikariTransactor
import doobie.implicits._
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import fs2._

import scala.concurrent.duration._

class PostgresEventJournalEventsByTagGapTest extends FunSuite with Matchers with BeforeAndAfterAll {
  implicit val contextShift =
    IO.contextShift(scala.concurrent.ExecutionContext.global)

  implicit val timer = IO.timer(scala.concurrent.ExecutionContext.global)

  val stringSerializer: Serializer[String] = new Serializer[String] {
    override def serialize(a: String): (TypeHint, Array[Byte]) =
      ("", a.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    override def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, String] =
      Right(new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
  }

//  private val xa = Transactor.fromDriverManager[IO](
//    "org.postgresql.Driver",
//    s"jdbc:postgresql://localhost:5432/postgres",
//    "user",
//    ""
//  )

  private val createTransactor = for {
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

  val schema = JournalSchema(s"test_${UUID.randomUUID().toString.replace('-', '_')}")
  val tagging = Tagging.partitioned[String](10)(EventTag("test-skip"))
  val journal = PostgresEventJournal(schema, tagging, stringSerializer).transactK(xa)
  val queries = PostgresEventJournalQueries[String](schema, stringSerializer, 100.millis, xa)
  override protected def beforeAll(): Unit =
    schema.createTable.transact(xa).unsafeRunSync()

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
    (schema.dropTable.transact(xa) >> shutdownTransactor).unsafeRunSync()

}
