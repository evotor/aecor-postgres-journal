package aecor.runtime.postgres

import cats.implicits._
import cats.kernel.Hash
import cats.tagless.FunctorK
import cats.tagless.syntax.functorK._
import cats.{ Monad, ~> }
import doobie._
import doobie.implicits._

object PostgresRuntime {
  def runBehavior[K: Hash, M[_[_]]: FunctorK, F[_]: Monad](typeName: String,
                                                           behavior: K => M[ConnectionIO],
                                                           transactor: Transactor[F]): K => M[F] = {
    key =>
      val lockKey =
        sql"""
             /*NO LOAD BALANCE*/
             SELECT pg_advisory_xact_lock(${typeName.hashCode}, ${key.hash})"""
          .query[Unit]
          .unique
      behavior(key).mapK(Lambda[ConnectionIO ~> F] { action =>
        (lockKey >> action).transact(transactor)
      })
  }
}