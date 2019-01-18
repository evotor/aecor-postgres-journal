package aecor.runtime.postgres

import cats.implicits._
import cats.tagless.FunctorK
import cats.tagless.syntax.functorK._
import cats.{Monad, ~>}
import doobie._
import doobie.implicits._

object PostgresRuntime {
  def runBehavior[K, M[_[_]]: FunctorK, F[_]: Monad](
      typeName: String,
      behavior: K => M[ConnectionIO],
      transactor: Transactor[F]
  ): K => M[F] = {
    key =>
      behavior(key).mapK(Lambda[ConnectionIO ~> F] { action =>
        (sql"SELECT pg_advisory_xact_lock(${typeName.hashCode}, ${key.hashCode()})"
          .query[Unit]
          .unique >>
          action).transact(transactor)
      })
  }
}
