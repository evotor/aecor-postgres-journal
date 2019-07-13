package aecor.runtime.postgres.account

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets

import aecor.MonadActionReject
import aecor.data.Folded.syntax._
import aecor.data._
import aecor.journal.postgres.PostgresEventJournal.Serializer
import aecor.journal.postgres.PostgresEventJournal.Serializer.TypeHint
import aecor.runtime.postgres.account.AccountEvent.{AccountCredited, AccountDebited, AccountOpened}
import aecor.runtime.postgres.account.EventsourcedAlgebra.AccountState
import aecor.runtime.postgres.account.Rejection.{AccountDoesNotExist, InsufficientFunds}
import cats.Monad
import cats.implicits._
import io.circe.jawn
import io.circe.syntax._
import io.circe.generic.auto._

final class EventsourcedAlgebra[F[_]](
  implicit F: MonadActionReject[F, Option[AccountState], AccountEvent, Rejection]
) extends Algebra[F] {

  import F._

  override def open(checkBalance: Boolean): F[Unit] =
    read.flatMap {
      case None =>
        append(AccountOpened(checkBalance))
      case Some(_) =>
        ().pure[F]
    }

  override def credit(transactionId: TransactionId, amount: BigDecimal): F[Unit] =
    read.flatMap {
      case Some(account) =>
        if (account.hasProcessedTransaction(transactionId)) {
          ().pure[F]
        } else {
          append(AccountCredited(transactionId, amount))
        }
      case None =>
        reject(AccountDoesNotExist)
    }

  override def debit(transactionId: TransactionId, amount: BigDecimal): F[Unit] =
    read.flatMap {
      case Some(account) =>
        if (account.hasProcessedTransaction(transactionId)) {
          ().pure[F]
        } else {
          if (account.hasFunds(amount)) {
            append(AccountDebited(transactionId, amount))
          } else {
            reject(InsufficientFunds)
          }
        }
      case None =>
        reject(AccountDoesNotExist)
    }

  override def getBalance: F[BigDecimal] = read.flatMap {
    case Some(account) =>
      account.balance.pure[F]
    case None =>
      reject(AccountDoesNotExist)
  }
}

object EventsourcedAlgebra {

  def behavior[F[_]: Monad]: EventsourcedBehavior[EitherK[Algebra, Rejection, ?[_]], F, Option[
    AccountState
  ], AccountEvent] =
    EventsourcedBehavior
      .rejectable(new EventsourcedAlgebra, AccountState.fold)

  final val rootAccountId: AccountId = AccountId("ROOT")
  final case class AccountState(balance: BigDecimal, processedTransactions: Set[TransactionId], checkBalance: Boolean) {
    def hasProcessedTransaction(transactionId: TransactionId): Boolean =
      processedTransactions.contains(transactionId)
    def hasFunds(amount: BigDecimal): Boolean =
      !checkBalance || balance >= amount
    def update(event: AccountEvent): Folded[AccountState] = event match {
      case AccountOpened(_) => impossible
      case AccountDebited(transactionId, amount) =>
        copy(
          balance = balance - amount,
          processedTransactions = processedTransactions + transactionId
        ).next
      case AccountCredited(transactionId, amount) =>
        copy(
          balance = balance + amount,
          processedTransactions = processedTransactions + transactionId
        ).next
    }
  }
  object AccountState {
    def fromEvent(event: AccountEvent): Folded[AccountState] = event match {
      case AccountOpened(checkBalance) => AccountState(0, Set.empty, checkBalance).next
      case _                           => impossible
    }
    implicit val serializer: Serializer[AccountState] = new Serializer[AccountState] {
      override def serialize(a: AccountState): (TypeHint, Array[Byte]) =
        ("", a.asJson.noSpaces.getBytes(StandardCharsets.UTF_8))
      override def deserialize(typeHint: TypeHint, bytes: Array[Byte]): Either[Throwable, AccountState] =
        jawn
          .parseByteBuffer(ByteBuffer.wrap(bytes))
          .flatMap(_.as[AccountState])
    }
    val fold: Fold[Folded, Option[AccountState], AccountEvent] =
      Fold.optional(fromEvent)(_.update(_))
  }
}
