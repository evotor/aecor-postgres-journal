package aecor.runtime.postgres.account
import aecor.encoding.{KeyDecoder, KeyEncoder}
import cats.kernel.Hash
import cats.tagless.FunctorK

trait Algebra[F[_]] {
  def open(checkBalance: Boolean): F[Unit]
  def credit(transactionId: TransactionId, amount: Amount): F[Unit]
  def debit(transactionId: TransactionId, amount: Amount): F[Unit]
  def getBalance: F[Amount]
}

object Algebra {
  implicit val catsTaglessFunctorK: FunctorK[Algebra] = cats.tagless.Derive.functorK[Algebra]
}

final case class AccountId(value: String) extends AnyVal

object AccountId {
  implicit val keyEncoder: KeyEncoder[AccountId] = KeyEncoder.anyVal[AccountId]
  implicit val keyDecoder: KeyDecoder[AccountId] = KeyDecoder.anyVal[AccountId]
  implicit val catsKernelHash: Hash[AccountId] = Hash.fromUniversalHashCode
}

final case class TransactionId(asString: String) extends AnyVal
final case class Amount(asLong: Long) extends AnyVal {
  def >(other: Amount): Boolean = asLong > other.asLong
  def <=(other: Amount): Boolean = asLong <= other.asLong
  def >=(other: Amount): Boolean = asLong >= other.asLong
  def -(other: Amount): Amount = Amount(asLong - other.asLong)
  def +(other: Amount): Amount = Amount(asLong + other.asLong)
}

object Amount {
  val zero: Amount = Amount(0)
}
