package aecor.journal.postgres

import aecor.data._
import aecor.encoding.{ KeyDecoder, KeyEncoder }
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.EventJournal
import cats.Monad
import cats.data.NonEmptyChain
import cats.effect.Timer
import cats.implicits.{ none, _ }
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.Stream

import scala.concurrent.duration.FiniteDuration

final class PostgresEventJournal[K, E](val schema: JournalSchema,
                                       tagging: Tagging[K],
                                       val serializer: Serializer[E])(implicit
                                                                      encodeKey: KeyEncoder[K])
    extends EventJournal[ConnectionIO, K, E] {

  import schema._

  implicit val keyWrite: Put[K] = Put[String].contramap(encodeKey(_))

  private val noLoadBalance = Update0("/*NO LOAD BALANCE*/", none).run

  private val appendQuery =
    Update[(K, Long, String, Array[Byte], List[String])](
      s"INSERT INTO $tableName (key, seq_nr, type_hint, bytes, tags) VALUES (?, ?, ?, ?, ?)"
    )

  override def append(key: K, offset: Long, events: NonEmptyChain[E]): ConnectionIO[Unit] = {

    val tags = tagging.tag(key).map(_.value).toList

    val lockTags = tags.traverse_ { tag =>
      sql"select pg_advisory_xact_lock(${tableName.hashCode}, ${tag.hashCode})"
        .query[Unit]
        .option
    }

    val insertEvents = events.traverseWithIndexM { (e, idx) =>
      val (typeHint, bytes) = serializer.serialize(e)
      appendQuery.run((key, idx + offset, typeHint, bytes, tags))
    }.void

    noLoadBalance >> lockTags >>
      insertEvents
  }

  override def foldById[S](key: K, offset: Long, zero: S)(
    f: (S, E) => Folded[S]
  ): ConnectionIO[Folded[S]] =
    (Stream.eval_(noLoadBalance) ++
      (fr"SELECT type_hint, bytes FROM"
        ++ Fragment.const(tableName)
        ++ fr"WHERE key = $key and seq_nr >= $offset ORDER BY seq_nr ASC")
        .query[(TypeHint, Array[Byte])]
        .stream)
      .evalMap {
        case (typeHint, bytes) =>
          AsyncConnectionIO.fromEither(serializer.deserialize(typeHint, bytes))
      }
      .scan(Folded.next(zero))((acc, e) => acc.flatMap(f(_, e)))
      .takeWhile(_.isNext, takeFailure = true)
      .compile
      .last
      .map {
        case Some(x) => x
        case None    => Folded.next(zero)
      }
}

object PostgresEventJournal {
  trait Serializer[A] {
    def serialize(a: A): (TypeHint, Array[Byte])
    def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, A]
  }
  object Serializer {
    type TypeHint = String
  }

  def apply[K: KeyEncoder: KeyDecoder, E](schema: JournalSchema,
                                          tagging: Tagging[K],
                                          serializer: Serializer[E]): PostgresEventJournal[K, E] =
    new PostgresEventJournal(schema, tagging, serializer)

  implicit final class PostgresEventJournalCIOTransactSyntax[K, E](
    val self: PostgresEventJournal[K, E]
  ) extends AnyVal {
    def transact[F[_]: Monad](xa: Transactor[F]): EventJournal[F, K, E] =
      new EventJournal[F, K, E] {
        override def append(entityKey: K, sequenceNr: Long, events: NonEmptyChain[E]): F[Unit] =
          self.append(entityKey, sequenceNr, events).transact(xa)
        override def foldById[S](entityKey: K, sequenceNr: Long, initial: S)(
          f: (S, E) => Folded[S]
        ): F[Folded[S]] =
          self.foldById(entityKey, sequenceNr, initial)(f).transact(xa)
      }
  }

  implicit final class PostgresEventJournalQueriesSyntax[K, E](val self: PostgresEventJournal[K, E])
      extends AnyVal {
    def queries[F[_]: Monad: Timer](pollingInterval: FiniteDuration, xa: Transactor[F])(
      implicit K: KeyDecoder[K]
    ): PostgresEventJournalQueries[F, K, E] =
      PostgresEventJournalQueries[K](self.schema, self.serializer, pollingInterval, xa)
  }

}
