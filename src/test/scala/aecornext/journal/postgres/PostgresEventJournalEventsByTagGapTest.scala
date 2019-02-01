package aecornext.journal.postgres

import java.util.UUID

import aecornext.data._
import aecornext.journal.postgres.PostgresEventJournal.Serializer
import aecornext.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import cats.data.NonEmptyChain
import cats.effect.IO
import cats.implicits._
import doobie.util.transactor.Transactor
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import scala.concurrent.duration._

class PostgresEventJournalEventsByTagGapTest
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {

  implicit val timer = IO.timer(scala.concurrent.ExecutionContext.global)
  val stringSerializer: Serializer[String] = new Serializer[String] {
    override def serialize(a: String): (TypeHint, Array[Byte]) =
      ("", a.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    override def deserialize(typeHint: TypeHint,
                             bytes: Array[Byte]): Either[Throwable, String] =
      Right(new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
  }

  private val xa = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    s"jdbc:postgresql://localhost:5432/postgres",
    "user",
    ""
  )

  val tagging = Tagging.const[String](EventTag("test-skip"))
  val journal = PostgresEventJournal(
    xa,
    PostgresEventJournal.Settings(
      tableName = s"test_${UUID.randomUUID().toString.replace('-', '_')}",
      pollingInterval = 100.millis),
    tagging,
    stringSerializer
  )
  val consumerId = ConsumerId("C-skip")

  override protected def beforeAll(): Unit = {
    journal.createTable.unsafeRunSync()
  }

  test("Journal doesn't allow gap in eventsByTag during concurrent writes") {
    val eventsForEachAggregate = 50L

    def aggergateEvents(id: String) =
      (1L to eventsForEachAggregate).map(i => (i, id, s"$id$i")).toList

    val allEvents =
      (0 until 30).toList.map("a" + _.toString).map(aggergateEvents)
    val allEventsCount = allEvents.size * eventsForEachAggregate

    def appendEvents(events: List[(Long, String, String)]) = events.traverse {
      case (offset, id, content) =>
        journal.append(id, offset, NonEmptyChain(content))
    }

    val appendAllEvents = allEvents.parTraverse(appendEvents).void

    val foldEvents = journal
      .eventsByTag(tagging.tag, Offset(0L))
      .takeWhile(_._1.value != allEventsCount)
      .compile
      .fold(Vector.empty[(Offset, EntityEvent[String, String])])(_ :+ _)

    val x = for {
      fiber <- foldEvents.start
      _ <- appendAllEvents
      out <- fiber.join
    } yield out

    val diff =
      (1L until allEventsCount).toVector.diff(x.unsafeRunSync().map(_._1.value))
    assert(diff.isEmpty)
  }

  override protected def afterAll(): Unit = {
    journal.dropTable.unsafeRunSync()
  }

}
