package aecor.journal.postgres

import java.util.UUID

import aecor.data._
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.postgres.PostgresEventJournal.Serializer
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
    tagging,
    stringSerializer
  )
  val consumerId = ConsumerId("C1")

  override protected def beforeAll(): Unit = {
    journal.createTable.unsafeRunSync()
  }

  test("Journal appends and folds events from zero offset") {
    val x = for {
      _ <- journal.append("a", 1L, NonEmptyVector.of("1", "2"))
      folded <- journal.foldById("a", 1L, Vector.empty[String])((acc, e) =>
        Folded.next(acc :+ e))
    } yield folded

    x.unsafeRunSync() should be(Folded.next(Vector("1", "2")))
  }

  test("Journal appends and folds events from non-zero offset") {
    val x = for {
      _ <- journal.append("a", 3L, NonEmptyVector.of("3"))
      folded <- journal.foldById("a", 2L, Vector.empty[String])((acc, e) =>
        Folded.next(acc :+ e))
    } yield folded

    x.unsafeRunSync() should be(Folded.next(Vector("2", "3")))
  }

  test("Journal rejects append at existing offset") {
    val x = journal.append("a", 3L, NonEmptyVector.of("4"))
    intercept[PSQLException] {
      x.unsafeRunSync()
    }
  }

  test("Journal emits current events by tag") {
    val x = for {
      _ <- journal.append("b", 1L, NonEmptyVector.of("b1"))
      _ <- journal.append("a", 4L, NonEmptyVector.of("a4"))
      folded <- journal
        .currentEventsByTag(tagging.tag, Offset(0L))
        .compile
        .fold(Vector.empty[(Offset, EntityEvent[String, String])])(_ :+ _)
    } yield folded

    val expected = Vector(
      (Offset(1L), EntityEvent("a", 1, "1")),
      (Offset(2L), EntityEvent("a", 2, "2")),
      (Offset(3L), EntityEvent("a", 3, "3")),
      (Offset(5L), EntityEvent("b", 1, "b1")),
      (Offset(6L), EntityEvent("a", 4, "a4"))
    )

    assert(x.unsafeRunSync() == expected)
  }

  test("Journal emits current events by tag from non zero offset") {
    val x = journal
      .currentEventsByTag(tagging.tag, Offset(3L))
      .compile
      .fold(Vector.empty[(Offset, EntityEvent[String, String])])(_ :+ _)

    val expected =
      Vector((Offset(3L), EntityEvent("a", 3, "3")),
             (Offset(5L), EntityEvent("b", 1, "b1")),
             (Offset(6L), EntityEvent("a", 4, "a4")))

    assert(x.unsafeRunSync() == expected)
  }

  test("Journal continuosly emits events by tag") {
    val appendEvent =
      journal.append("b", 2L, NonEmptyVector.of("b2"))
    val foldEvents = journal
      .eventsByTag(tagging.tag, Offset(0L))
      .take(6)
      .compile
      .fold(Vector.empty[(Offset, EntityEvent[String, String])])(_ :+ _)

    val x = for {
      fiber <- foldEvents.start
      _ <- appendEvent
      out <- fiber.join
    } yield out

    val expected = Vector(
      (Offset(1L), EntityEvent("a", 1l, "1")),
      (Offset(2L), EntityEvent("a", 2l, "2")),
      (Offset(3L), EntityEvent("a", 3l, "3")),
      (Offset(5L), EntityEvent("b", 1l, "b1")),
      (Offset(6L), EntityEvent("a", 4l, "a4")),
      (Offset(7L), EntityEvent("b", 2l, "b2"))
    )

    assert(x.unsafeRunSync() == expected)
  }

  test("Journal continuosly emits events by tag from non zero offset inclusive") {
    val appendEvent =
      journal.append("a", 5L, NonEmptyVector.of("a5"))
    val foldEvents = journal
      .eventsByTag(tagging.tag, Offset(7L))
      .take(2)
      .compile
      .fold(Vector.empty[(Offset, EntityEvent[String, String])])(_ :+ _)

    val x = for {
      fiber <- foldEvents.start
      _ <- appendEvent
      out <- fiber.join
    } yield out

    val expected = Vector(
      (Offset(7L), EntityEvent("b", 2l, "b2")),
      (Offset(8L), EntityEvent("a", 5l, "a5"))
    )

    assert(x.unsafeRunSync() == expected)
  }

  test("Journal correctly uses offset store for current events by tag") {
    val x = for {
      os <- TestOffsetStore(
        Map(TagConsumer(tagging.tag, consumerId) -> Offset(3L)))
      runOnce = fs2.Stream
        .force(
          journal
            .withOffsetStore(os)
            .currentEventsByTag(tagging.tag, consumerId))
        .evalMap(_.commit)
        .as(1)
        .compile
        .fold(0)(_ + _)
      processed1 <- runOnce
      _ <- journal.append("a", 6L, NonEmptyVector.of("a6"))
      processed2 <- runOnce
    } yield (processed1, processed2)

    assert(x.unsafeRunSync == ((4, 1)))
  }

  override protected def afterAll(): Unit = {
    journal.dropTable.unsafeRunSync()
  }

}
