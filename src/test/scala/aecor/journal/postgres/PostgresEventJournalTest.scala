package aecor.journal.postgres

import java.util.UUID

import aecor.data.{EntityEvent, EventTag, Folded, Tagging}
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.postgres.PostgresEventJournal.{EntityName, Serializer}
import cats.data.NonEmptyVector
import cats.effect.IO
import org.postgresql.util.PSQLException
import org.scalatest.{BeforeAndAfterAll, FunSuite, Matchers}
import cats.implicits._
import doobie.util.transactor.Transactor

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

class PostgresEventJournalTest
    extends FunSuite
    with Matchers
    with BeforeAndAfterAll {
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
    "notxcain",
    "1"
  )

  val tagging = Tagging.const[String](EventTag("test"))
  val journal = PostgresEventJournal(
    xa,
    PostgresEventJournal.Settings(
      tableName = s"test_${UUID.randomUUID().toString.replace('-', '_')}",
      pollingInterval = 1.second
    ),
    EntityName("test"),
    tagging,
    stringSerializer
  )

  override protected def beforeAll(): Unit = {
    journal.createTable.unsafeRunSync()
  }

  test("Journal appends and folds events from zero offset") {
    val x = for {
      _ <- journal.append("a", 0L, NonEmptyVector.of("1", "2"))
      folded <- journal.foldById("a", 0L, Vector.empty[String])((acc, e) =>
        Folded.next(acc :+ e))
    } yield folded

    x.unsafeRunSync() should be(Folded.next(Vector("1", "2")))
  }

  test("Journal appends and folds events from non-zero offset") {
    val x = for {
      _ <- journal.append("a", 2L, NonEmptyVector.of("3"))
      folded <- journal.foldById("a", 1L, Vector.empty[String])((acc, e) =>
        Folded.next(acc :+ e))
    } yield folded

    x.unsafeRunSync() should be(Folded.next(Vector("2", "3")))
  }

  test("Journal rejects append at existing offset") {
    val x = journal.append("a", 2L, NonEmptyVector.of("4"))
    intercept[PSQLException] {
      x.unsafeRunSync()
    }
  }

  test("Journal emits current events by tag") {
    val x = for {
      _ <- journal.append("b", 0L, NonEmptyVector.of("b1"))
      _ <- journal.append("a", 3L, NonEmptyVector.of("a4"))
      folded <- journal
        .currentEventsByTag(tagging.tag, 0L)
        .compile
        .fold(Vector.empty[(Long, EntityEvent[String, String])])(_ :+ _)
    } yield folded

    val expected = Vector((1L, EntityEvent("a", 1, "1")),
                          (2L, EntityEvent("a", 2, "2")),
                          (3L, EntityEvent("a", 3, "3")),
                          (5L, EntityEvent("b", 1, "b1")),
                          (6L, EntityEvent("a", 4, "a4")))

    assert(x.unsafeRunSync() == expected)
  }

  test("Journal emits current events by tag from non zero offset") {
    val x = journal
      .currentEventsByTag(tagging.tag, 3L)
      .compile
      .fold(Vector.empty[(Long, EntityEvent[String, String])])(_ :+ _)

    val expected =
      Vector((5L, EntityEvent("b", 1, "b1")), (6L, EntityEvent("a", 4, "a4")))

    assert(x.unsafeRunSync() == expected)
  }

  test("Journal continuosly emits events by tag") {
    val appendEvent =
      journal.append("b", 1L, NonEmptyVector.of("b2"))
    val foldEvents = journal
      .eventsByTag(tagging.tag, 0L)
      .take(6)
      .compile
      .fold(Vector.empty[(Long, EntityEvent[String, String])])(_ :+ _)

    val x = for {
      fiber <- foldEvents.start
      _ <- appendEvent
      out <- fiber.join
    } yield out

    val expected = Vector(
      (1L, EntityEvent("a", 1l, "1")),
      (2L, EntityEvent("a", 2l, "2")),
      (3L, EntityEvent("a", 3l, "3")),
      (5L, EntityEvent("b", 1l, "b1")),
      (6L, EntityEvent("a", 4l, "a4")),
      (7L, EntityEvent("b", 2l, "b2"))
    )

    assert(x.unsafeRunSync() == expected)
  }

  test("Journal continuosly emits events by tag from non zero offset") {
    val appendEvent =
      journal.append("a", 4L, NonEmptyVector.of("a5"))
    val foldEvents = journal
      .eventsByTag(tagging.tag, 6L)
      .take(2)
      .compile
      .fold(Vector.empty[(Long, EntityEvent[String, String])])(_ :+ _)

    val x = for {
      fiber <- foldEvents.start
      _ <- appendEvent
      out <- fiber.join
    } yield out

    val expected = Vector(
      (7L, EntityEvent("b", 2l, "b2")),
      (8L, EntityEvent("a", 5l, "a5"))
    )

    assert(x.unsafeRunSync() == expected)
  }

  override protected def afterAll(): Unit = {
    journal.dropTable.unsafeRunSync()
  }

}
