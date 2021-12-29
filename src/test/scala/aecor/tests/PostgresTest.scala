package aecor.tests

import java.util.UUID

import cats.effect.std.Dispatcher
import cats.effect.syntax.all._
import cats.effect.{Async, Resource}
import cats.syntax.all._
import com.dimafeng.testcontainers.{ForAllTestContainer, PostgreSQLContainer}
import com.zaxxer.hikari.HikariConfig
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.util.ExecutionContexts
import doobie.util.fragment.Fragment
import doobie.{ConnectionIO, FC}
import org.scalatest.{Assertion, AsyncTestSuite}
import org.testcontainers.utility.DockerImageName

import scala.concurrent.Future
import scala.util.Try

trait PostgresTest[F[_]] extends ForAllTestContainer { self: AsyncTestSuite =>
  override val container: PostgreSQLContainer = {
    val runningContainer =
      PostgreSQLContainer
        .Def(dockerImageName = DockerImageName.parse("postgres:13.3-alpine"))
        .start()
    sys.addShutdownHook(runningContainer.stop())
    runningContainer
  }

  def effectTest(testCase: => F[Assertion])(implicit D: Dispatcher[F]): Future[Assertion] =
    D.unsafeToFuture(testCase)

  def newDatabaseResource(implicit A: Async[F]): Resource[F, HikariTransactor[F]] =
    createTransactorWithNewSchema(container)

  private def createTransactor(
    driverClassName: String,
    url: String,
    user: String,
    password: String,
    schema: Option[String],
    maximumPoolSize: Int
  )(implicit A: Async[F]): Resource[F, HikariTransactor[F]] =
    for {
      hikariConfig <- Resource.eval(Async[F].fromTry {
        Try {
          val config = new HikariConfig()
          config.setDriverClassName(driverClassName)
          config.setJdbcUrl(url)
          config.setUsername(user)
          config.setPassword(password)
          config.setAutoCommit(false)
          schema.foreach(config.setSchema)
          config.setMaximumPoolSize(maximumPoolSize)
          config
        }
      })
      connectEC <- ExecutionContexts.fixedThreadPool[F](maximumPoolSize)
      transactor <- HikariTransactor.fromHikariConfig[F](
        hikariConfig = hikariConfig,
        connectEC = connectEC
      )
    } yield transactor

  private def createTransactorWithNewSchema(
    underlying: PostgreSQLContainer
  )(implicit A: Async[F]): Resource[F, HikariTransactor[F]] = {
    def withoutTransaction[A](p: ConnectionIO[A]): ConnectionIO[A] =
      FC.setAutoCommit(true).bracket(_ => p)(_ => FC.setAutoCommit(false))

    val createNewSchema = createTransactor(
      driverClassName = underlying.driverClassName,
      url = underlying.jdbcUrl,
      user = underlying.username,
      password = underlying.password,
      schema = none,
      maximumPoolSize = 1
    ).use { transactor =>
      val databaseName =
        s"test_schema_${UUID.randomUUID().toString.replace('-', '_')}"

      transactor.trans.apply {
        withoutTransaction {
          Fragment
            .const(s"CREATE SCHEMA $databaseName")
            .update
            .run
            .as(databaseName)
        }
      }
    }

    Resource.eval(createNewSchema).flatMap { databaseName =>
      createTransactor(
        driverClassName = underlying.driverClassName,
        url = underlying.jdbcUrl,
        user = underlying.username,
        password = underlying.password,
        schema = databaseName.some,
        maximumPoolSize = 10
      )
    }
  }
}
