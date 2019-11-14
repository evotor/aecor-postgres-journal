package aecor.journal.postgres

import aecor.data._
import aecor.encoding.KeyEncoder
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.EventJournal
import cats.data.NonEmptyChain
import cats.effect.Bracket
import cats.implicits.{none, _}
import doobie._
import doobie.implicits._
import doobie.postgres.implicits._
import fs2.Stream

final class PostgresEventJournal[K, E](tableName: String, tagging: Tagging[K], serializer: Serializer[E])(
  implicit
  encodeKey: KeyEncoder[K]
) extends EventJournal[ConnectionIO, K, E] { self =>

  implicit val keyWrite: Put[K] = Put[String].contramap(encodeKey(_))

  private val appendQuery =
    Update[(K, Long, String, Array[Byte], List[String])](
      s"INSERT INTO $tableName (key, seq_nr, type_hint, bytes, tags) VALUES (?, ?, ?, ?, ?)"
    )

  override def append(key: K, offset: Long, events: NonEmptyChain[E]): ConnectionIO[Unit] = {

    val tags = tagging.tag(key).map(_.value).toList

    val lockTags = tags.traverse_ { tag =>
      sql"SELECT pg_advisory_xact_lock(${tableName.hashCode}, ${tag.hashCode})"
        .query[Unit]
        .option
    }

    val insertEvents = events.traverseWithIndexM { (e, idx) =>
      val (typeHint, bytes) = serializer.serialize(e)
      appendQuery.run((key, idx + offset, typeHint, bytes, tags))
    }.void

    lockTags >>
      insertEvents
  }

  override def read(key: K, offset: Long): Stream[doobie.ConnectionIO, EntityEvent[K, E]] =
    (fr"SELECT type_hint, bytes, seq_nr FROM"
      ++ Fragment.const(tableName)
      ++ fr"WHERE key = $key and seq_nr >= $offset ORDER BY seq_nr ASC")
      .query[(TypeHint, Array[Byte], Long)]
      .stream
      .flatMap {
        case (typeHint, bytes, seqNr) =>
          Stream.fromEither[ConnectionIO](serializer.deserialize(typeHint, bytes)).map { e =>
            EntityEvent(key, seqNr, e)
          }
      }

  def transactK[F[_]: Bracket[*[_], Throwable]](xa: Transactor[F]): EventJournal[F, K, E] =
    new EventJournal[F, K, E] {
      override def append(entityKey: K, sequenceNr: Long, events: NonEmptyChain[E]): F[Unit] =
        self.append(entityKey, sequenceNr, events).transact(xa)
      override def read(key: K, offset: Long): Stream[F, EntityEvent[K, E]] =
        self.read(key, offset).transact(xa)
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

  def apply[K: KeyEncoder, E](tableName: String,
                              tagging: Tagging[K],
                              serializer: Serializer[E]): PostgresEventJournal[K, E] =
    new PostgresEventJournal(tableName, tagging, serializer)

  /**
    * For PgPool users. Modifies Transactor Strategy.
    * Adds /*NO LOAD BALANCE*/ directive at the beginning of each transaction
    * which routes queries to master server
    * Use this function for transactor that is used for a write side
    */
  def addNoLoadBalanceDirective[F[_]](xa: Transactor[F]): Transactor[F] = {
    val noLoadBalance = Update0("/*NO LOAD BALANCE*/", none).run
    val oldStrategy = xa.strategy
    val newStrategy = oldStrategy.copy(before = noLoadBalance *> oldStrategy.before)
    xa.copy(strategy0 = newStrategy)
  }

}
