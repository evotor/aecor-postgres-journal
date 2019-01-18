package aecor.runtime.postgres.account

sealed abstract class Rejection extends Product with Serializable

object Rejection {
  case object AccountDoesNotExist extends Rejection
  case object InsufficientFunds extends Rejection
  case object HoldNotFound extends Rejection
}
