package aecor.runtime.postgres.account

import cats.tagless.autoFunctorK

final case class TransactionId(asString: String) extends AnyVal
final case class Amount(asBigDecimal: BigDecimal) extends AnyVal {
  def >(other: Amount): Boolean = asBigDecimal > other.asBigDecimal
  def <=(other: Amount): Boolean = asBigDecimal <= other.asBigDecimal
  def >=(other: Amount): Boolean = asBigDecimal >= other.asBigDecimal
  def -(other: Amount): Amount = Amount(asBigDecimal - other.asBigDecimal)
  def +(other: Amount): Amount = Amount(asBigDecimal + other.asBigDecimal)
}

object Amount {
  val zero: Amount = Amount(0)
}

@autoFunctorK(true)
trait Algebra[F[_]] {
  def open(checkBalance: Boolean): F[Unit]
  def credit(transactionId: TransactionId, amount: Amount): F[Unit]
  def debit(transactionId: TransactionId, amount: Amount): F[Unit]
  def getBalance: F[Amount]
}

object Algebra
