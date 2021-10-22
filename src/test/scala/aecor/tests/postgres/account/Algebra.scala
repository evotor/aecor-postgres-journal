package aecor.tests.postgres.account
import aecor.encoding.{ KeyDecoder, KeyEncoder }
import cats.kernel.Hash
import cats.tagless.{ Derive, FunctorK }

trait Algebra[F[_]] {
  def open(checkBalance: Boolean): F[Unit]
  def credit(transactionId: TransactionId, amount: BigDecimal): F[Unit]
  def debit(transactionId: TransactionId, amount: BigDecimal): F[Unit]
  def getBalance: F[BigDecimal]
}

object Algebra {
  implicit val catsTaglessFunctorK: FunctorK[Algebra] = Derive.functorK[Algebra]
}

final case class AccountId(value: String) extends AnyVal

object AccountId {
  implicit val keyEncoder: KeyEncoder[AccountId] = KeyEncoder.anyVal[AccountId]
  implicit val keyDecoder: KeyDecoder[AccountId] = KeyDecoder.anyVal[AccountId]
  implicit val catsKernelHash: Hash[AccountId] = Hash.fromUniversalHashCode
}

final case class TransactionId(asString: String) extends AnyVal
