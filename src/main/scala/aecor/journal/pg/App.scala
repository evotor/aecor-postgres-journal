package aecor.journal.pg

import aecor.data.{EventTag, Folded, Tagging}
import aecor.journal.pg.PostgresEventJournal.Serializer.TypeHint
import aecor.journal.pg.PostgresEventJournal.{
  ConnectionSettings,
  EntityName,
  Serializer
}

import cats.data.NonEmptyVector
import cats.effect.IO

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import cats.implicits._
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

  val tagging = Tagging.partitioned[String](10)(EventTag("test"))

  val journal = PostgresEventJournal[IO](settings,
                                         EntityName("test"),
                                         tagging,
                                         stringSerializer)

  def loadSeqNr(key: String): IO[Long] =
    journal
      .foldById(key, 0l, 0L)((s, _) => Folded.next(s + 1))
      .flatMap(
        _.fold(IO.raiseError[Long](new IllegalStateException("Lol")))(IO.pure))

  def rec(key: String, seqNr: Long): IO[Unit] =
    for {
      _ <- journal.append(key, seqNr, NonEmptyVector.one(s"$key - $seqNr"))
      _ <- IO.sleep(100.millis)
      _ <- rec(key, seqNr + 1)
    } yield ()

  def emitEvents(key: String) = loadSeqNr(key).flatMap(rec(key, _))

  def printEvents(tag: EventTag): IO[Unit] =
    IO(println(s"Starting consumption of tag `${tag.value}`")) >>
      journal
        .eventsByTag(tag, 0L)
        .evalMap(x => IO(println(x)))
        .compile
        .drain

  def runPar(ps: Vector[IO[Unit]]): IO[Unit] =
    ps.parSequence.void

  def program =
    for {
      _ <- runPar(
        Vector(emitEvents("2"),
               emitEvents("3"),
               emitEvents("1"),
               emitEvents("4"),
               emitEvents("5"),
               emitEvents("6")) ++
          tagging.tags.toVector
            .map(printEvents))

    } yield ()

  program.unsafeRunSync()
}
