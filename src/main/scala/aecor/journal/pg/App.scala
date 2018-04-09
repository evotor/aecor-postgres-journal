package aecor.journal.pg

import aecor.data.{EventTag, Folded, Tagging}
import aecor.journal.pg.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.pg.PostgresEventJournal.{
  ConnectionSettings,
  EntityName,
  Serializer
}
import cats.data.NonEmptyVector
import cats.effect.{IO}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object App extends App {

  val settings = PostgresEventJournal.Settings(
    connectionSettings = ConnectionSettings("localhost",
                                            5432,
                                            "postgres",
                                            "events",
                                            "notxcain",
                                            "1"),
    pollingInterval = 1.second
  )

  val stringSerializer: Serializer[String] = new Serializer[String] {
    override def serialize(a: String): (TypeHint, Array[Byte]) =
      ("", a.getBytes(java.nio.charset.StandardCharsets.UTF_8))

    override def deserialize(typeHint: TypeHint,
                             bytes: Array[Byte]): Either[Throwable, String] =
      Right(new String(bytes, java.nio.charset.StandardCharsets.UTF_8))
  }

  val tag = EventTag("test")

  val journal = PostgresEventJournal[IO](settings,
                                         EntityName("test"),
                                         Tagging.const[String](tag),
                                         stringSerializer)

  def loadSeqNr(key: String): IO[Long] =
    journal
      .foldById(key, 0l, 0L)((s, _) => Folded.next(s + 1))
      .flatMap(
        _.fold(IO.raiseError[Long](new IllegalStateException("Lol")))(IO.pure))

  def rec(key: String, seqNr: Long): IO[Unit] =
    for {
      _ <- journal.append(key, seqNr, NonEmptyVector.one(s"$seqNr"))
      _ <- IO.sleep(100.millis)
      _ <- rec(key, seqNr + 1)
    } yield ()

  def emitEvents(key: String) = loadSeqNr(key).flatMap(rec(key, _))

  def printEvents: IO[Unit] =
    journal
      .eventsByTag(tag, 0L)
      .evalMap(x => IO(println(x)))
      .compile
      .drain

  def program(key: String) =
    for {
      _ <- IO.race(emitEvents(key), printEvents)
    } yield ()

  program("3").unsafeRunSync()
}
